from typing import Dict
import polars as pl
import logging

from message_pack_parser.core.exceptions import AggregationError
from message_pack_parser.core.stats.types import Stat

logger = logging.getLogger(__name__)

# --- Configuration ---
FRAME_RATE = 30  # game ticks per second
INTERVAL_SECONDS = 15  # sampling interval


def force_int_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    If a column is a struct, unnest it. Always ensures the final column is Int64.
    """
    if col not in df.columns:
        return df

    if isinstance(df[col].dtype, pl.Struct):
        # Unnest only the target column. Passing a list is safer.
        df = df.unnest([col])

    # Always cast after any possible unnesting.
    return df.with_columns(pl.col(col).cast(pl.Int64))


# ──────────────────────────────────────────────────────────────────────────────
# Main stat function
# ──────────────────────────────────────────────────────────────────────────────
def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    logger.info("Calculating stat: Army Value Timeline")

    # 1. Inputs
    unit_events_df = dataframes["unit_events"]
    unit_defs_df = dataframes["unit_defs"]
    defs_map_df = dataframes["defs_map"]

    print("\n--- [1] INPUTS ---")
    print("unit_events_df:", unit_events_df.shape, unit_events_df.schema)
    print("unit_defs_df:  ", unit_defs_df.shape, unit_defs_df.schema)
    print("defs_map_df:   ", defs_map_df.shape, defs_map_df.schema)

    # 2. Clean frame
    unit_events_df = force_int_column(unit_events_df, "frame")
    print("\n--- [2] AFTER force_int_column(frame) ---")
    print(unit_events_df.shape, unit_events_df.schema)

    # 3. Clean maps
    clean_defs_map = defs_map_df.select(["unit_def_id", "unit_name"])
    clean_unit_defs = unit_defs_df.select(["unit_name", "metalcost"])
    print("\n--- [3] CLEAN MAPS ---")
    print("defs_map:", clean_defs_map.shape, clean_defs_map.schema)
    print("unit_defs:", clean_unit_defs.shape, clean_unit_defs.schema)

    # 4. Max frame
    frame_col = unit_events_df["frame"].cast(pl.Int64)
    max_frame_value = frame_col.max()
    if max_frame_value is None:
        max_frame = 0
    elif isinstance(max_frame_value, (int, float)):
        max_frame = int(max_frame_value)
    else:
        max_frame = 0
    print(f"\n--- [4] max_frame = {max_frame} ---")

    # 5. Lifespans
    finished = (
        unit_events_df.filter(pl.col("event_type") == "FINISHED")
        .select(["frame", "unit_id", "unit_def_id", "unit_team_id"])
        .rename({"frame": "creation_frame"})
    )
    destroyed = (
        unit_events_df.filter(pl.col("event_type") == "DESTROYED")
        .select(["frame", "unit_id"])
        .rename({"frame": "death_frame"})
    )
    unit_lifespans = finished.join(destroyed, on="unit_id", how="left").with_columns(
        pl.col("death_frame").fill_null(max_frame + 1)
    )
    print("\n--- [5a] AFTER JOIN lifespans ---")
    print(unit_lifespans.shape, unit_lifespans.schema)

    unit_lifespans = force_int_column(unit_lifespans, "death_frame")
    print("\n--- [5b] AFTER force_int_column(death_frame) ---")
    print(unit_lifespans.shape, unit_lifespans.schema)

    # 6. Attach costs
    jm1 = unit_lifespans.join(clean_defs_map, on="unit_def_id", how="left")
    print("\n--- [6a] AFTER JOIN defs_map ---")
    print(jm1.shape, jm1.schema)

    jm2 = jm1.join(clean_unit_defs, on="unit_name", how="left")
    print("\n--- [6b] AFTER JOIN unit_defs ---")
    print(jm2.shape, jm2.schema)

    unit_lifespans_with_cost = jm2.filter(
        pl.col("unit_name").is_not_null()
    ).with_columns(pl.col("metalcost").fill_null(0.0).cast(pl.Float64))
    print("\n--- [6c] AFTER fill & cast metalcost ---")
    print(unit_lifespans_with_cost.shape, unit_lifespans_with_cost.schema)

    unit_lifespans_with_cost = force_int_column(unit_lifespans_with_cost, "death_frame")
    print("\n--- [6d] AFTER defense force_int_column(death_frame) ---")
    print(unit_lifespans_with_cost.shape, unit_lifespans_with_cost.schema)

    # early exit
    if unit_lifespans_with_cost.is_empty():
        print("\n--- [6e] No data, early exit ---")
        return pl.DataFrame(
            schema={"team_id": pl.Int64, "frame": pl.Int64, "army_value": pl.Float64}
        )

    # 7. Creation & destruction deltas
    creations_delta = unit_lifespans_with_cost.select(
        pl.col("creation_frame").alias("frame"),
        pl.col("unit_team_id").alias("team_id"),
        pl.col("metalcost").alias("value_change"),
    )
    print("\n--- [7] creations_delta ---")
    print(creations_delta.shape, creations_delta.schema)

    tmp = unit_lifespans_with_cost.filter(pl.col("death_frame") <= max_frame)
    print("\n--- [8] post-filter lifespans_for_destroy ---")
    print(tmp.shape, tmp.schema)

    destructions_delta = tmp.select(
        pl.col("death_frame").alias("frame"),
        pl.col("unit_team_id").alias("team_id"),
        (-pl.col("metalcost")).alias("value_change"),
    )
    print("\n--- [8b] destructions_delta ---")
    print(destructions_delta.shape, destructions_delta.schema)

    all_value_changes = pl.concat([creations_delta, destructions_delta])
    print("\n--- [9] all_value_changes ---")
    print(all_value_changes.shape, all_value_changes.schema)

    army_value_log = (
        all_value_changes.group_by(["team_id", "frame"])
        .agg(pl.sum("value_change").alias("net_change_at_frame"))
        .sort(["team_id", "frame"])
        .with_columns(
            pl.col("net_change_at_frame").cum_sum().over("team_id").alias("army_value")
        )
        .select(["team_id", "frame", "army_value"])
    )
    print("\n--- [10] army_value_log ---")
    print(army_value_log.shape, army_value_log.schema)

    # 8. Build timeline
    interval_frames = INTERVAL_SECONDS * FRAME_RATE
    base_tl = pl.DataFrame(
        {
            "frame": pl.int_range(
                0, max_frame, interval_frames, eager=True, dtype=pl.Int64
            )
        }
    )
    last_tl = pl.DataFrame({"frame": [max_frame]})
    timeline_df = pl.concat([base_tl, last_tl]).unique(subset=["frame"]).sort("frame")
    print("\n--- [11] timeline_df ---")
    print(timeline_df.shape, timeline_df.schema)

    # 9. Cross-join with teams
    all_teams = army_value_log["team_id"].unique(maintain_order=True)
    print("\n--- [12] all_teams ---")
    print("team count:", all_teams.len())
    expanded_tl = timeline_df.join(
        pl.DataFrame({"team_id": all_teams}), how="cross"
    ).sort(["team_id", "frame"])
    print("\n--- [13] expanded_timeline ---")
    print(expanded_tl.shape, expanded_tl.schema)

    # 10. As-of join
    print("\n--- [14] ABOUT TO join_asof ---")
    print("left-schema:", expanded_tl.schema)
    print("right-schema:", army_value_log.schema)

    # 6. As-of Join and Final Cleanup
    joined_df = expanded_tl.join_asof(army_value_log, on="frame", by="team_id")

    # THE FIX: Defensively cast the 'army_value' column after the join_asof operation.
    # This creates a "firebreak", sanitizing the column type before any further
    # operations are performed on it, preventing the schema corruption error.
    sanitized_df = joined_df.with_columns(
        pl.col("army_value").cast(pl.Float64, strict=False)
    )

    final = (
        sanitized_df
        .with_columns(pl.col("army_value").forward_fill().over("team_id"))
        .fill_null({"army_value": 0.0})
        .with_columns(
            pl.when(pl.col("army_value") < 0)
              .then(0.0)
              .otherwise(pl.col("army_value"))
              .alias("army_value")
        )
        .select(["team_id", "frame", "army_value"])
    )


    return final


# ──────────────────────────────────────────────────────────────────────────────
# Stat registration object
# ──────────────────────────────────────────────────────────────────────────────
STAT_DEFINITION = Stat(
    func=calculate,
    description="Calculates the total army value for each team at fixed time intervals.",
    default_enabled=True,
)
