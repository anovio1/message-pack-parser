"""
Step 7: Final Output Generation Orchestrator
"""
import logging
import polars as pl

from message_pack_parser.core.output_strategies import OutputStrategy

logger = logging.getLogger(__name__)

def generate_output(
    strategy: OutputStrategy,
    aggregated_df: pl.DataFrame,
    unaggregated_df: pl.DataFrame,
    output_directory: str,
    replay_id: str
) -> None:
    """
    Orchestrates the final output generation by executing a provided strategy.

    This function is decoupled from the specific output format.

    Args:
        strategy: An instance of a class that implements the OutputStrategy interface.
        aggregated_df: The DataFrame containing aggregated data.
        unaggregated_df: The DataFrame containing detailed, unaggregated data.
        output_directory: The base directory where output should be saved.
        replay_id: The unique identifier for the replay.
    """
    logger.info("Starting Step 7: Final Output Generation")
    try:
        strategy.write(
            aggregated_df=aggregated_df,
            unaggregated_df=unaggregated_df,
            output_directory=output_directory,
            replay_id=replay_id
        )
    except Exception as e:
        # The specific strategy will raise a more detailed error
        logger.error(f"Output generation failed: {e}", exc_info=True)
        # Re-raise to be caught by main error handler
        raise e