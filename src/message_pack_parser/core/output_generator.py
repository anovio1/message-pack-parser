# src/message_pack_parser/core/output_generator.py
"""
Step 7: Output Orchestrator

This module acts as the entry point for the final output step.
Its sole responsibility is to invoke the `write` method of a given
output strategy, passing along the computed data streams.
"""
import logging
import polars as pl
from typing import Dict, Tuple, Any

from message_pack_parser.core.output_strategies import OutputStrategy
from message_pack_parser.core.exceptions import OutputGenerationError

logger = logging.getLogger(__name__)


def generate_output(
    strategy: OutputStrategy,
    transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
    transformed_unaggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
    output_directory: str,
    replay_id: str,
) -> None:
    """
    Orchestrates the final output generation by executing a provided strategy.

    Args:
        strategy: OutputStrategy.
        aggregated_stats: A dictionary [data name: aggregated DataFrame]
        unaggregated_df: The DataFrame containing detailed, unaggregated data.
        output_directory: The base directory where output should be saved.
        replay_id: The unique identifier for the replay.
    """
    logger.info(f"Executing output generation using '{strategy.__class__.__name__}'.")

    try:
        # Delegate the entire write operation to the strategy object.
        # Its signature perfectly matches what we pass here.
        strategy.write(
            transformed_aggregated_data=transformed_aggregated_data,
            transformed_unaggregated_data=transformed_unaggregated_data,
            output_directory=output_directory,
            replay_id=replay_id,
        )
    except Exception as e:
        # Catch exceptions from the strategy and re-raise as a generic error
        # for the main error handler. The strategy will have logged specifics.
        logger.error(
            f"Output generation failed for strategy '{strategy.__class__.__name__}'",
            exc_info=True,
        )
        raise OutputGenerationError(
            f"Output generation failed for strategy '{strategy.__class__.__name__}'"
        ) from e
