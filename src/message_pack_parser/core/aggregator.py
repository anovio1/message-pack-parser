"""Step 6: Data Aggregation and Stream Generation"""
from typing import Dict, Tuple
import polars as pl
import logging

logger = logging.getLogger(__name__)

def _perform_passthrough_aggregation(dataframes: Dict[str, pl.DataFrame]) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger.warning("No production aggregation logic defined. Concatenating all data into the 'unaggregated' stream.")
    all_dfs = [df.with_columns(pl.lit(name).alias("source_aspect")) for name, df in dataframes.items() if not df.is_empty()]
    if not all_dfs:
        return pl.DataFrame(), pl.DataFrame()
    return pl.DataFrame(), pl.concat(all_dfs, how="diagonal")

def perform_aggregations(dataframes: Dict[str, pl.DataFrame], run_demo_logic: bool = False) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger.info("Starting Step 6: Data Aggregation")
    if run_demo_logic:
        # Same illustrative logic as before...
        logger.warning("Running ILLUSTRATIVE aggregation logic.")
        agg_df = pl.DataFrame(); unagg_df = pl.DataFrame()
        if "team_stats" in dataframes and not dataframes["team_stats"].is_empty():
            agg_df = dataframes["team_stats"].group_by("team_id").agg(pl.sum("metal_produced").alias("total_metal_produced")).sort("team_id")
        if "unit_positions" in dataframes and not dataframes["unit_positions"].is_empty():
            unagg_df = dataframes["unit_positions"].sort("frame")
        return agg_df, unagg_df

    # Default behavior is now a safe pass-through, not NotImplementedError
    return _perform_passthrough_aggregation(dataframes)