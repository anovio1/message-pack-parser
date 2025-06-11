"""Step 6: Data Aggregation and Stream Generation"""
from typing import Dict, Tuple
import polars as pl
import logging
from message_pack_parser.core.exceptions import AggregationError

logger = logging.getLogger(__name__)

def _perform_passthrough_aggregation(dataframes: Dict[str, pl.DataFrame]) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger.warning("No production aggregation logic defined. Concatenating all data into the 'unaggregated' stream.")
    all_dfs = [df.with_columns(pl.lit(name).alias("source_aspect")) for name, df in dataframes.items() if not df.is_empty()]
    if not all_dfs:
        return pl.DataFrame(), pl.DataFrame()
    return pl.DataFrame(), pl.concat(all_dfs, how="diagonal")

def perform_aggregations(dataframes_by_aspect: Dict[str, pl.DataFrame], run_demo_logic: bool = False) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger.info("Starting Step 6: Data Aggregation")
    # The default behavior raises an error to ensure production logic is implemented.
    # We will replace it with our new, specific aggregation.
    if run_demo_logic:
        logger.warning("Running ILLUSTRATIVE aggregation logic. This is NOT for production.")
        # Fallback to a simple demo if the flag is passed
        agg_df = _calculate_damage_by_unit_def(dataframes_by_aspect.get("damage_log"))
        unagg_df = dataframes_by_aspect.get("unit_events", pl.DataFrame())
        return agg_df, unagg_df

    # --- PRODUCTION AGGREGATION LOGIC ---
    try:
        # **This is where you implement your primary aggregations.**
        # For this example, our primary aggregation IS the damage calculation.
        aggregated_df = _calculate_damage_by_unit_def(dataframes_by_aspect.get("damage_log"))

        # For the unaggregated stream, we can pass through a relevant, detailed log.
        # Let's pass through the entire damage log for detailed inspection.
        unaggregated_df = dataframes_by_aspect.get("damage_log", pl.DataFrame())

        if aggregated_df.is_empty():
            logger.warning("Aggregation resulted in an empty DataFrame.")
            
        logger.info(f"Aggregation complete. Aggregated shape: {aggregated_df.shape}, Unaggregated shape: {unaggregated_df.shape}")
        return aggregated_df, unaggregated_df
        
    except Exception as e:
        logger.error(f"An error occurred during the production aggregation process: {e}", exc_info=True)
        raise AggregationError("Aggregation failed") from e


    # Default behavior is now a safe pass-through, not NotImplementedError
    return _perform_passthrough_aggregation(dataframes)


def _calculate_damage_by_unit_def(damage_log_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the total damage dealt by each unique attacker unit definition ID.

    Args:
        damage_log_df: The cleaned DataFrame from the 'damage_log' aspect.

    Returns:
        A Polars DataFrame with columns ['unit_def_id', 'total_damage_dealt'],
        sorted by damage in descending order.
    """
    if damage_log_df is None or damage_log_df.is_empty():
        logger.warning("Damage log DataFrame is missing or empty. Cannot calculate damage by unit definition.")
        # Return an empty DataFrame with the expected schema
        return pl.DataFrame(schema={'unit_def_id': pl.Int64, 'total_damage_dealt': pl.Float64})

    logger.info("Calculating total damage per attacker unit definition ID.")
    
    # Core Logic: Filter -> Group By -> Aggregate -> Rename -> Sort
    damage_by_def = (
        damage_log_df
        # 1. Filter for rows where damage is attributed to a specific unit definition
        .filter(pl.col("attacker_def_id").is_not_null())
        # 2. Group by the attacker's unit definition ID
        .group_by("attacker_def_id")
        # 3. Aggregate by summing the damage for each group
        .agg(
            pl.sum("damage").alias("total_damage_dealt")
        )
        # 4. Rename the grouping column for clarity in the final output
        .rename({"attacker_def_id": "unit_def_id"})
        # 5. Sort the results to see the highest-damage units first
        .sort("total_damage_dealt", descending=True)
    )
    
    logger.info(f"Successfully aggregated damage for {damage_by_def.height} unique unit definitions.")
    return damage_by_def
