import logging
import time
from typing import List, Dict, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed

import typer
import polars as pl
from rich.progress import Progress
from enum import Enum

# Import from our package
from message_pack_parser.logging_config import setup_logging
from message_pack_parser.core.ingestion import load_mpk_files, list_recognized_aspects
from message_pack_parser.core.decoder import stream_decode_aspect
from message_pack_parser.core.cache_manager import save_to_cache, load_from_cache
from message_pack_parser.core.value_transformer import stream_transform_aspect
from message_pack_parser.core.dataframe_creator import create_polars_dataframe_for_aspect
from message_pack_parser.core.aggregator import perform_aggregations
from message_pack_parser.core.output_generator import generate_output
from message_pack_parser.core.output_strategies import (
    OutputStrategy,
    MessagePackGzipStrategy,
    ParquetDirectoryStrategy,
    JsonLinesGzipStrategy
)
from message_pack_parser.schemas.aspects_raw import BaseAspectDataPointRaw
from message_pack_parser.core.exceptions import ParserError, CacheValidationError
from message_pack_parser.utils.config_validator import validate_configurations

# Enum for CLI choices
class OutputFormat(str, Enum):
    MPK_GZIP = "mpk-gzip"
    PARQUET_DIR = "parquet-dir"
    JSONL_GZIP = "jsonl-gzip"

# Map for strategy selection
STRATEGY_MAP: Dict[OutputFormat, type[OutputStrategy]] = {
    OutputFormat.MPK_GZIP: MessagePackGzipStrategy,
    OutputFormat.PARQUET_DIR: ParquetDirectoryStrategy,
    OutputFormat.JSONL_GZIP: JsonLinesGzipStrategy,
}


app = typer.Typer(
    help="A production-grade, parallelized parser for game replay data.",
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)
logger = logging.getLogger(__name__)

# --- PARALLEL EXECUTION LOGIC ---
def _parallel_decode_and_transform(aspect_name: str, raw_bytes: bytes, skip_on_error: bool):
    """Worker function for parallel processing: Decodes and transforms a single aspect."""
    raw_stream = stream_decode_aspect(aspect_name, raw_bytes, skip_on_error)
    transformed_stream = stream_transform_aspect(aspect_name, raw_stream, skip_on_error)
    return aspect_name, list(transformed_stream)

def _run_parallel_pipeline(
    raw_mpk_data: Dict[str, bytes],
    skip_on_error: bool
) -> Dict[str, pl.DataFrame]:
    """Runs Steps 2-5 of the pipeline in parallel, sacrificing caching for performance."""
    logger.warning("Running in parallel mode. Caching of intermediate raw data is disabled.")
    
    # Heuristic: only parallelize files over a certain size
    SERIAL_PROCESSING_THRESHOLD_BYTES = 10 * 1024 # 10 KB
    aspects_to_parallelize = {k: v for k, v in raw_mpk_data.items() if len(v) > SERIAL_PROCESSING_THRESHOLD_BYTES}
    aspects_to_run_serially = {k: v for k, v in raw_mpk_data.items() if len(v) <= SERIAL_PROCESSING_THRESHOLD_BYTES}
    
    dataframes: Dict[str, pl.DataFrame] = {}

    # Run small aspects serially to avoid process overhead
    if aspects_to_run_serially:
        logger.info(f"Processing {len(aspects_to_run_serially)} small aspects serially...")
        for name, data in aspects_to_run_serially.items():
            _, transformed_models = _parallel_decode_and_transform(name, data, skip_on_error)
            dataframes[name] = create_polars_dataframe_for_aspect(name, transformed_models)

    # Run large aspects in parallel
    if aspects_to_parallelize:
        logger.info(f"Processing {len(aspects_to_parallelize)} large aspects in parallel...")
        with Progress() as progress:
            task = progress.add_task("[cyan]Decoding & Transforming...", total=len(aspects_to_parallelize))
            with ProcessPoolExecutor() as executor:
                # Submit decode & transform tasks first
                tf_futures = {executor.submit(_parallel_decode_and_transform, name, data, skip_on_error): name for name, data in aspects_to_parallelize.items()}
                transformed_data = {}
                for future in as_completed(tf_futures):
                    name, models = future.result()
                    transformed_data[name] = models
                    progress.update(task, advance=1)

            # Now create DataFrames (can also be parallelized if CPU intensive)
            task_df = progress.add_task("[green]Creating DataFrames...", total=len(transformed_data))
            for name, models in transformed_data.items():
                dataframes[name] = create_polars_dataframe_for_aspect(name, models)
                progress.update(task_df, advance=1)
                
    return dataframes


# --- SERIAL EXECUTION LOGIC ---
def _run_serial_pipeline(
    raw_mpk_data: Dict[str, bytes],
    cache_dir: str,
    replay_id: str,
    use_cache: bool,
    force_reprocess: bool,
    skip_on_error: bool
) -> Dict[str, pl.DataFrame]:
    """Runs Steps 2-5 of the pipeline sequentially, enabling caching."""
    logger.info("Running in serial mode. Caching is enabled.")

    # Step 2 & 3: Try loading from cache first
    raw_data_by_aspect = None
    if use_cache and not force_reprocess:
        try:
            raw_data_by_aspect = load_from_cache(cache_dir, replay_id)
        except (CacheValidationError, ParserError) as e:
            logger.warning(f"Could not use cache: {e}. Reprocessing from raw files.")

    if raw_data_by_aspect is None:
        # If cache miss or force reprocess, perform decoding (Step 2)
        logger.info("Performing serial decoding for all aspects...")
        raw_data_by_aspect = {
            name: list(stream_decode_aspect(name, data, skip_on_error))
            for name, data in raw_mpk_data.items()
        }
        # And save to cache (Step 3)
        if use_cache:
            save_to_cache(raw_data_by_aspect, cache_dir, replay_id)

    # Step 4: Transformation
    logger.info("Performing serial value transformation...")
    transformed_data = {
        name: list(stream_transform_aspect(name, iter(models), skip_on_error))
        for name, models in raw_data_by_aspect.items()
    }
    
    # Step 5: DataFrame Creation
    logger.info("Performing serial DataFrame creation...")
    dataframes = {
        name: create_polars_dataframe_for_aspect(name, models)
        for name, models in transformed_data.items()
    }

    return dataframes


@app.command()
def run(
    replay_id: str = typer.Argument(..., help="A unique identifier for the replay."),
    input_dirs: List[str] = typer.Option(..., "--input-dir", "-i", help="Input directory. Can be used multiple times."),
    cache_dir: str = typer.Option(..., "--cache-dir", "-c", help="Directory for intermediate cached data."),
    output_dir: str = typer.Option(..., "--output-dir", "-o", help="Directory for the final compressed output."),
    output_format: OutputFormat = typer.Option(OutputFormat.MPK_GZIP, "--output-format", "-f", help="The format for the final output.", case_sensitive=False),
    serial: bool = typer.Option(
        False, 
        "--serial", 
        help="Run in single-threaded mode. Disables parallelism but enables caching and simplifies debugging."
    ),
    no_cache: bool = typer.Option(False, help="Disable using the cache (only effective in serial mode)."),
    force_reprocess: bool = typer.Option(False, help="Force reprocessing, ignoring existing cache (only effective in serial mode)."),
    skip_on_error: bool = typer.Option(False, help="Skip individual records that fail validation instead of halting."),
    run_demo_aggregation: bool = typer.Option(False, help="Run illustrative aggregation logic instead of production logic."),
    log_level: str = typer.Option("INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)."),
    dry_run: bool = typer.Option(False, help="Validate config and list input files without processing.")
):
    """
    Runs the complete Message Pack parsing pipeline for a given replay.
    """
    total_start_time = time.perf_counter()
    setup_logging(log_level)
    
    try:
        logger.info("--- [Step 0] Configuration Validation ---")
        validate_configurations()

        logger.info("--- [Step 1] File Ingestion ---")
        raw_mpk_data = load_mpk_files(input_dirs)
        if not raw_mpk_data:
            raise ParserError("Step 1 Ingestion Error: No MPK files were loaded.")
        logger.info(f"Ingested {len(raw_mpk_data)} aspect files.")
        
        if dry_run:
            logger.info("Dry run requested. Found the following aspects:")
            for aspect_name, raw_bytes in raw_mpk_data.items():
                logger.info(f"  - {aspect_name} ({len(raw_bytes) / 1024:.2f} KB)")
            logger.info("Dry run complete. No data processed.")
            return

        # --- ROUTING LOGIC: Choose between serial and parallel execution ---
        dataframes: Dict[str, pl.DataFrame]
        stage_start_time = time.perf_counter()
        
        if serial:
            dataframes = _run_serial_pipeline(raw_mpk_data, cache_dir, replay_id, not no_cache, force_reprocess, skip_on_error)
        else:
            dataframes = _run_parallel_pipeline(raw_mpk_data, skip_on_error)
            
        logger.info(f"Main processing (Steps 2-5) complete in {time.perf_counter() - stage_start_time:.2f}s.")

        # --- Steps 6 & 7 are always serial ---
        logger.info("--- [Step 6] Data Aggregation ---")
        stage_start_time = time.perf_counter()
        aggregated_df, unaggregated_df = perform_aggregations(dataframes, run_demo_aggregation)
        logger.info(f"Stage complete in {time.perf_counter() - stage_start_time:.2f}s.")
        
        logger.info("--- [Step 7] Final Output Generation ---")
        stage_start_time = time.perf_counter()
        strategy_instance = STRATEGY_MAP[output_format]()
        generate_output(strategy_instance, aggregated_df, unaggregated_df, output_dir, replay_id)
        logger.info(f"Stage complete in {time.perf_counter() - stage_start_time:.2f}s.")

    except ParserError as e:
        logger.critical(f"A fatal parser error occurred: {e}", exc_info=False)
        if isinstance(e, CacheValidationError):
            logger.critical("Suggestion: Try re-running with the --force-reprocess flag.")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.critical(f"An unexpected fatal error occurred: {e}", exc_info=True)
        raise typer.Exit(code=1)
    
    total_time = time.perf_counter() - total_start_time
    logger.info(f"--- Pipeline finished successfully for Replay ID: {replay_id} in {total_time:.2f} seconds ---")


@app.command(name="list-aspects")
def cli_list_aspects():
    """Lists all aspect names recognized by the current schemas."""
    print("Recognized aspect schemas:")
    for aspect in list_recognized_aspects():
        print(f"  - {aspect}")

if __name__ == "__main__":
    app()