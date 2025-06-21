"""
Step 6: Data Aggregation and Stream Generation Orchestrator
"""

from typing import Dict, Tuple, List
import polars as pl
import logging

from .exceptions import AggregationError

# Import the dynamically built registries from the stats package
from .stats import STATS_REGISTRY, UNAGGREGATED_STREAM_REGISTRY

logger = logging.getLogger(__name__)


def perform_aggregations(
    dataframes_by_aspect: Dict[str, pl.DataFrame],
    stats_to_compute: List[str],
    unaggregated_streams_to_compute: List[
        str
    ],  # <-- NEW: Argument to control which streams to generate
) -> Tuple[Dict[str, pl.DataFrame], Dict[str, pl.DataFrame]]:
    """
    Orchestrates the execution of requested statistics using the dynamic registry.
    If `stats_to_compute` is empty, all stats marked as `default_enabled` are run.
    """
    logger.info("Starting Step 6: Configurable Aggregation")

    if not stats_to_compute:
        stats_to_compute = [
            name for name, stat in STATS_REGISTRY.items() if stat.default_enabled
        ]
        logger.warning(
            f"No specific stats requested. Computing default set: {stats_to_compute}"
        )

    logger.info(f"Computing stats: {stats_to_compute}")

    computed_stats: Dict[str, pl.DataFrame] = {}
    for stat_name in stats_to_compute:
        if stat_name in STATS_REGISTRY:
            stat = STATS_REGISTRY[stat_name]
            try:
                result_df = stat.func(dataframes_by_aspect)
                if result_df is not None and not result_df.is_empty():
                    computed_stats[stat_name] = result_df
                else:
                    logger.warning(
                        f"Stat '{stat_name}' produced an empty or null DataFrame."
                    )
            except Exception as e:
                logger.error(
                    f"Error calculating stat '{stat_name}': {e}", exc_info=True
                )
        else:
            logger.warning(
                f"Requested stat '{stat_name}' is not in STATS_REGISTRY. Skipping."
            )

    logger.info(f"Generating unaggregated streams: {unaggregated_streams_to_compute}")
    computed_unaggregated_streams: Dict[str, pl.DataFrame] = {}
    for stream_name in unaggregated_streams_to_compute:
        if stream_name in UNAGGREGATED_STREAM_REGISTRY:
            stream_func = UNAGGREGATED_STREAM_REGISTRY[stream_name]
            try:
                # Correctly CALL the function to get the DataFrame
                result_df = stream_func(dataframes_by_aspect)
                if result_df is not None and not result_df.is_empty():
                    computed_unaggregated_streams[stream_name] = result_df
                else:
                    logger.warning(
                        f"Unaggregated stream '{stream_name}' produced an empty or null DataFrame."
                    )
            except Exception as e:
                logger.error(
                    f"Error generating unaggregated stream '{stream_name}': {e}",
                    exc_info=True,
                )
        else:
            logger.warning(
                f"Requested unaggregated stream '{stream_name}' is not in registry. Skipping."
            )

    logger.info(
        f"Aggregation complete. Computed {len(computed_stats)} aggregated stats and {len(computed_unaggregated_streams)} unaggregated streams."
    )
    return computed_stats, computed_unaggregated_streams
