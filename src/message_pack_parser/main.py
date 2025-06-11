"""Main CLI entry point for the Message Pack Parser application."""
import logging; import time; from typing import List, Dict; from concurrent.futures import ProcessPoolExecutor, as_completed
import typer; import polars as pl
from rich.progress import Progress

from message_pack_parser.logging_config import setup_logging
from message_pack_parser.core.ingestion import load_mpk_files, list_recognized_aspects
from message_pack_parser.core.decoder import stream_decode_aspect
from message_pack_parser.core.cache_manager import save_to_cache, load_from_cache
from message_pack_parser.core.value_transformer import stream_transform_aspect
from message_pack_parser.core.dataframe_creator import create_polars_dataframe_for_aspect
from message_pack_parser.core.aggregator import perform_aggregations
from message_pack_parser.core.output_generator import generate_final_output
from message_pack_parser.core.exceptions import ParserError, CacheValidationError
from message_pack_parser.utils.config_validator import validate_configurations

app = typer.Typer(help="A production-grade, parallelized parser for game replay data.", context_settings={"help_option_names": ["-h", "--help"]}, add_completion=False)
logger = logging.getLogger(__name__)
SERIAL_PROCESSING_THRESHOLD_BYTES = 10 * 1024 # 10 KB

def _process_aspect_serially(aspect_name: str, raw_bytes: bytes, skip_on_error: bool) -> pl.DataFrame:
    raw_stream = stream_decode_aspect(aspect_name, raw_bytes, skip_on_error)
    transformed_stream = stream_transform_aspect(aspect_name, raw_stream, skip_on_error)
    return create_polars_dataframe_for_aspect(aspect_name, list(transformed_stream))

@app.command()
def run(
    replay_id: str = typer.Argument(..., help="A unique identifier for the replay."),
    input_dirs: List[str] = typer.Option(..., "--input-dir", "-i", help="Input directory. Can be used multiple times."),
    cache_dir: str = typer.Option(..., "--cache-dir", "-c", help="Directory for intermediate cached data."),
    output_dir: str = typer.Option(..., "--output-dir", "-o", help="Directory for the final compressed output."),
    no_cache: bool = typer.Option(False, help="Disable loading from and saving to the cache."),
    force_reprocess: bool = typer.Option(False, help="Ignore existing cache and force reprocessing."),
    skip_on_error: bool = typer.Option(False, help="Skip individual records that fail validation instead of halting."),
    run_demo_aggregation: bool = typer.Option(False, help="Run illustrative aggregation logic instead of the safe pass-through default."),
    log_level: str = typer.Option("INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)."),
    dry_run: bool = typer.Option(False, help="Validate config and list input files without processing data.")
):
    """Runs the complete Message Pack parsing pipeline for a given replay."""
    total_start_time = time.perf_counter(); setup_logging(log_level)
    try:
        logger.info("--- [Step 0] Configuration Validation ---"); validate_configurations()

        logger.info("--- [Step 1] File Ingestion ---")
        raw_mpk_data = load_mpk_files(input_dirs)
        if not raw_mpk_data: raise ParserError("No MPK files were loaded.")
        logger.info(f"Ingested {len(raw_mpk_data)} aspect files.")

        if dry_run:
            logger.info("Dry run requested. Found aspects:")
            for name, data in raw_mpk_data.items(): logger.info(f"  - {name} ({len(data) / 1024:.2f} KB)")
            return

        # For simplicity in this 3.0 refactor, cache is disabled when using parallel processing
        # as it adds significant complexity to manage pre-vs-post transform states.
        # A "4.0" might add post-transform caching (e.g., to Parquet).

        logger.info("--- [Steps 2, 4, 5] Parallel Decoding, Transformation, and DataFrame Creation ---")
        stage_start_time = time.perf_counter()
        dataframes: Dict[str, pl.DataFrame] = {}

        to_parallelize = {k: v for k, v in raw_mpk_data.items() if len(v) > SERIAL_PROCESSING_THRESHOLD_BYTES}
        to_run_serially = {k: v for k, v in raw_mpk_data.items() if len(v) <= SERIAL_PROCESSING_THRESHOLD_BYTES}

        if to_run_serially:
            logger.info(f"Processing {len(to_run_serially)} small aspects serially...")
            for name, data in to_run_serially.items(): dataframes[name] = _process_aspect_serially(name, data, skip_on_error)

        if to_parallelize:
            logger.info(f"Processing {len(to_parallelize)} large aspects in parallel...")
            with Progress() as progress:
                task = progress.add_task("[cyan]Processing aspects...", total=len(to_parallelize))
                # Important for Windows/macOS compatibility to have this inside the guarded block
                if __name__ == "__main__":
                    with ProcessPoolExecutor() as executor:
                        futures = {executor.submit(_process_aspect_serially, name, data, skip_on_error): name for name, data in to_parallelize.items()}
                        for future in as_completed(futures):
                            aspect_name = futures[future]
                            try: dataframes[aspect_name] = future.result()
                            except Exception as e: raise ParserError(f"A parallel task failed for aspect '{aspect_name}'") from e
                            progress.update(task, advance=1)

        logger.info(f"Dataframe creation stage complete in {time.perf_counter() - stage_start_time:.2f}s.")

        logger.info("--- [Step 6] Data Aggregation ---")
        stage_start_time = time.perf_counter()
        agg_df, unagg_df = perform_aggregations(dataframes, run_demo_aggregation)
        logger.info(f"Aggregation stage complete in {time.perf_counter() - stage_start_time:.2f}s.")
        
        print("unagg_df", len(unagg_df))

        logger.info("--- [Step 7] Final Output Generation ---")
        stage_start_time = time.perf_counter()
        generate_final_output(agg_df, unagg_df, output_dir, replay_id)
        logger.info(f"Output stage complete in {time.perf_counter() - stage_start_time:.2f}s.")

    except ParserError as e:
        logger.critical(f"A fatal parser error occurred: {e}", exc_info=True)
        if isinstance(e, CacheValidationError): logger.critical("Suggestion: Try re-running with the --force-reprocess flag.")
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
    for aspect in list_recognized_aspects(): print(f"  - {aspect}")

if __name__ == "__main__":
    app()