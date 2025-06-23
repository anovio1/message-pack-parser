# File: src/message_pack_parser/core/stats/unit_economic_contribution.py

import polars as pl
from typing import Dict, Tuple
from .types import Stat # Correct import path

def _calculate_accumulated_unit_economic_contribution_with_lifetime(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Calculates the total net economic contribution (metal and energy) and total lifetime
    for each unit type per player, by summing the accumulated economic activity and durations
    over each unit's lifespan.

    This version is robust to missing 'energy_make'/'energy_use' columns by conditionally
    calculating energy-related metrics.
    """
    unit_economy_df = dataframes.get("unit_economy")

    # Define the base schema for the final output DataFrame.
    # These are the columns that are expected to be present, even if they are null/zero.
    base_schema = {
        'team_id': pl.Int64,
        'unit_def_id': pl.Int64,
        'total_net_metal_contribution': pl.Float64,
        'total_net_energy_contribution': pl.Float64,
        'total_lifetime_seconds': pl.Float64,
        'total_units_produced': pl.UInt32,
    }

    # Return an empty DataFrame with the expected schema if no unit_economy data is found.
    if unit_economy_df is None or unit_economy_df.is_empty():
        return pl.DataFrame(schema=base_schema)

    # --- Data Preparation: Check column presence and build conditional expressions ---

    # Determine which economic metrics are available.
    metal_ok = {"metal_make", "metal_use"}.issubset(unit_economy_df.columns)
    energy_ok = {"energy_make", "energy_use"}.issubset(unit_economy_df.columns)

    # 1. Prepare processing expressions for net rates.
    processing_expressions = []
    if metal_ok:
        processing_expressions.append(
            (pl.col("metal_make").fill_null(0) - pl.col("metal_use").fill_null(0)).alias("net_metal_rate")
        )
    if energy_ok:
        processing_expressions.append(
            (pl.col("energy_make").fill_null(0) - pl.col("energy_use").fill_null(0)).alias("net_energy_rate")
        )

    # Apply the net rate calculations if any are possible.
    if not processing_expressions:
        # If neither metal nor energy columns are present, we can't calculate contributions.
        # We can still proceed to calculate lifetimes and counts.
        # To prevent downstream errors, we'll create a minimal processed_df and handle
        # empty contributions later.
        processed_df = unit_economy_df.select("unit_id", "team_id", "unit_def_id", "frame") # Minimal selection
        # Ensure base columns are present even if no rates are calculated
        if 'net_metal_rate' not in processed_df.columns:
             processed_df = processed_df.with_columns(pl.lit(0.0).alias('net_metal_rate'))
        if 'net_energy_rate' not in processed_df.columns:
             processed_df = processed_df.with_columns(pl.lit(0.0).alias('net_energy_rate'))
    else:
        processed_df = unit_economy_df.with_columns(processing_expressions)

    # 2. Sort data by unit_id and frame. This is crucial for correctly calculating
    # time intervals between consecutive events for each individual unit.
    sorted_df = processed_df.sort(["unit_id", "frame"])

    # --- Calculate Unit-Level Economic Contributions and Lifetimes ---

    # 3. Group by unit to get its first and last frame, and its defining IDs.
    unit_metadata = sorted_df.group_by(
        ["unit_id", "team_id", "unit_def_id"]
    ).agg([
        pl.first("frame").alias("first_frame"),
        pl.last("frame").alias("last_frame"),
    ])

    # 4. Calculate each unit's lifetime in seconds.
    unit_lifetimes = unit_metadata.with_columns([
        ((pl.col("last_frame") - pl.col("first_frame")) / 30.0).alias("unit_lifetime_seconds")
    ])

    # 5. Prepare for interval contribution calculation. Select necessary columns.
    interval_data_cols = ["unit_id", "team_id", "unit_def_id", "frame"]
    if metal_ok:
        interval_data_cols.append("net_metal_rate")
    if energy_ok:
        interval_data_cols.append("net_energy_rate")

    # Get the next frame for interval calculation. This is always needed if sorted_df is not empty.
    # Add next_frame regardless of economic columns, as it's used for filtering later.
    interval_data = sorted_df.select(interval_data_cols + [
        pl.col("frame").shift(-1).over("unit_id").alias("next_frame"),
    ])

    # 6. Calculate the contribution for each interval: rate * duration_in_seconds.
    interval_contribution_expressions = []
    if metal_ok:
        interval_contribution_expressions.append(
            (pl.col("net_metal_rate") * (pl.col("next_frame") - pl.col("frame")) / 30.0).alias("interval_metal_contrib")
        )
    if energy_ok:
        interval_contribution_expressions.append(
            (pl.col("net_energy_rate") * (pl.col("next_frame") - pl.col("frame")) / 30.0).alias("interval_energy_contrib")
        )

    # Apply the interval contribution calculations if any are possible.
    if interval_contribution_expressions:
        # Ensure frame_diff is calculated even if only metal_ok is true.
        interval_data = interval_data.with_columns(
            (pl.col("next_frame") - pl.col("frame")).alias("frame_diff")
        )
        interval_contributions = interval_data.with_columns(interval_contribution_expressions)
    else:
        # If no contributions can be calculated, create a placeholder for valid_intervals
        # that won't have contribution columns but can still be joined.
        # We need to ensure 'frame_diff' is also considered null if not calculated.
        interval_contributions = interval_data.select(interval_data_cols + ["next_frame"]).with_columns(
             pl.lit(None, dtype=pl.Int64).alias("frame_diff") # Explicitly null frame_diff
        )
        # Add empty contribution columns to ensure schema consistency if metal_ok or energy_ok are true
        # but no valid intervals were found (this is less likely given the filter step).
        if metal_ok and 'interval_metal_contrib' not in interval_contributions.columns:
            interval_contributions = interval_contributions.with_columns(pl.lit(0.0).alias('interval_metal_contrib'))
        if energy_ok and 'interval_energy_contrib' not in interval_contributions.columns:
            interval_contributions = interval_contributions.with_columns(pl.lit(0.0).alias('interval_energy_contrib'))


    # 7. Filter out incomplete intervals (where 'next_frame' is null)
    # and zero-duration intervals. This step is only meaningful if contributions exist.
    valid_intervals = pl.DataFrame() # Default to empty if no contribution expressions
    if interval_contribution_expressions:
        valid_intervals = interval_contributions.filter(
            pl.col("next_frame").is_not_null() & (pl.col("frame_diff") > 0)
        )


    # --- Aggregate Data to Player/UnitType Level ---

    # 8. Sum up the economic contributions for each unit instance.
    unit_economic_sums_exprs = []
    if metal_ok:
        unit_economic_sums_exprs.append(pl.sum("interval_metal_contrib").alias("total_unit_net_metal"))
    if energy_ok:
        unit_economic_sums_exprs.append(pl.sum("interval_energy_contrib").alias("total_unit_net_energy"))

    # Only perform aggregation if there are contribution expressions.
    unit_economic_sums = pl.DataFrame() # Initialize empty
    if unit_economic_sums_exprs and not valid_intervals.is_empty():
        unit_economic_sums = valid_intervals.group_by(
            ["unit_id", "team_id", "unit_def_id"]
        ).agg(unit_economic_sums_exprs)
    elif unit_economic_sums_exprs and valid_intervals.is_empty():
        # If expressions exist but no valid intervals, create an empty df with keys and zero sums
        unit_economic_sums = pl.DataFrame({
            "team_id": [], "unit_def_id": [], "unit_id": []
        }).group_by(["team_id", "unit_def_id", "unit_id"]).agg(unit_economic_sums_exprs)
    else:
         # If no contribution expressions, create an empty df with just keys for join
         unit_economic_sums = pl.DataFrame(schema={"team_id": pl.Int64, "unit_def_id": pl.Int64})


    # 9. Aggregate the economic sums to the (team_id, unit_def_id) level.
    player_economic_contributions_agg_exprs = []
    if metal_ok:
        player_economic_contributions_agg_exprs.append(
            pl.sum("total_unit_net_metal").alias("total_net_metal_contribution")
        )
    if energy_ok:
        player_economic_contributions_agg_exprs.append(
            pl.sum("total_unit_net_energy").alias("total_net_energy_contribution")
        )

    player_economic_contributions_agg = pl.DataFrame(schema={"team_id": pl.Int64, "unit_def_id": pl.Int64}) # Default empty schema
    if player_economic_contributions_agg_exprs and not unit_economic_sums.is_empty():
        player_economic_contributions_agg = unit_economic_sums.group_by(
            ["team_id", "unit_def_id"]
        ).agg(player_economic_contributions_agg_exprs)
    elif player_economic_contributions_agg_exprs and unit_economic_sums.is_empty():
         # If expressions exist but unit_economic_sums is empty, create an empty DF with keys and zero sums
         player_economic_contributions_agg = pl.DataFrame(schema={"team_id": pl.Int64, "unit_def_id": pl.Int64})


    # 10. Aggregate the calculated unit lifetimes and counts to the team and unit type level.
    # This step is independent of economic columns and should always run if unit_metadata is available.
    player_unit_lifetimes_counts = unit_lifetimes.group_by(
        ["team_id", "unit_def_id"]
    ).agg([
        pl.sum("unit_lifetime_seconds").alias("total_lifetime_seconds"),
        pl.len().alias("total_units_produced"), # Using pl.len() for robustness
    ])

    # --- Corrected Join and Finalization ---

    # Prepare DataFrames for a robust outer join with potentially null keys.
    # Rename keys in the 'right' DataFrame to avoid implicit column creation issues
    # and ensure explicit control over key merging.

    # Select aggregated economic data (left side of join)
    economic_data_cols = ["team_id", "unit_def_id"]
    # Only include economic columns if they were actually created.
    if metal_ok:
        economic_data_cols.append("total_net_metal_contribution")
    if energy_ok:
        economic_data_cols.append("total_net_energy_contribution")

    # Ensure we only select columns that exist in player_economic_contributions_agg.
    # This prevents ColumnNotFoundError if energy_ok was false and the aggregation
    # produced a DF without energy contribution columns.
    actual_economic_cols = [c for c in economic_data_cols if c in player_economic_contributions_agg.columns]
    economic_data = player_economic_contributions_agg.select(actual_economic_cols)

    # Select aggregated lifetime/count data (right side of join) and rename keys
    lifetime_data = player_unit_lifetimes_counts.select([
        pl.col("team_id").alias("team_id_r"), # Rename right-side keys to avoid conflict
        pl.col("unit_def_id").alias("unit_def_id_r"),
        "total_lifetime_seconds",
        "total_units_produced",
    ])

    # Perform the outer join.
    joined_df = economic_data.join(
        lifetime_data,
        left_on=["team_id", "unit_def_id"],
        right_on=["team_id_r", "unit_def_id_r"],
        how="outer"
    )

    # Coalesce the keys to create the final, correct team_id and unit_def_id columns.
    # This must happen *before* dropping the _r columns.
    final_agg_with_coalesced_keys = joined_df.with_columns([
        pl.col("team_id").fill_null(pl.col("team_id_r")).alias("team_id"),
        pl.col("unit_def_id").fill_null(pl.col("unit_def_id_r")).alias("unit_def_id"),
    ])

    # Drop the temporary aliased key columns.
    cleaned_agg = final_agg_with_coalesced_keys.drop(["team_id_r", "unit_def_id_r"])

    # 12. Fill nulls in the metric/count columns using the bullet-proof method.
    # Apply fill_null individually to each metric column using a list comprehension.
    metric_defaults = {
        "total_net_metal_contribution": 0.0,
        "total_net_energy_contribution": 0.0,
        "total_lifetime_seconds": 0.0,
        "total_units_produced": 0,
    }

    # Create a list of fill_null expressions, one for each metric column.
    # Only include expressions for columns that are actually present in the DataFrame.
    fill_expressions = [
        pl.col(c).fill_null(v).alias(c)
        for c, v in metric_defaults.items()
        if c in cleaned_agg.columns
    ]

    # Apply these expressions using with_columns.
    final_agg = cleaned_agg.with_columns(fill_expressions)

    # 13. Select and order the final columns for consistent output.
    # Ensure all desired final columns are requested, but only select those that exist.
    final_columns_to_select = [
        "team_id",
        "unit_def_id",
        "total_net_metal_contribution",
        "total_net_energy_contribution",
        "total_lifetime_seconds",
        "total_units_produced"
    ]
    # Filter the columns to select based on what's actually in the DataFrame
    existing_columns = [c for c in final_columns_to_select if c in final_agg.columns]

    # Ensure that if any of the core key columns are missing (e.g., due to
    # extremely malformed input), we still return a DataFrame with the expected keys.
    # If team_id or unit_def_id were missing from the coalescing, they'd be null.
    # We ensure they are in the final select if they are expected.
    final_output_columns = []
    if 'team_id' in existing_columns:
        final_output_columns.append('team_id')
    if 'unit_def_id' in existing_columns:
        final_output_columns.append('unit_def_id')
    
    # Add the metric columns that are guaranteed to exist (or be filled)
    final_output_columns.extend([c for c in existing_columns if c not in ['team_id', 'unit_def_id']])


    return final_agg.select(final_output_columns).sort(["team_id", "unit_def_id"])


# Define the Stat object for the aggregator system.
STAT_DEFINITION = Stat(
    func=_calculate_accumulated_unit_economic_contribution_with_lifetime,
    description="Calculates total net metal/energy contribution and total lifetime for each unit type per player.",
    default_enabled=False
)