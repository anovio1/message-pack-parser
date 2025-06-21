import os
import logging
import json
from typing import Any, List, Dict, Optional
from message_pack_parser.core.exceptions import FileIngestionError
from message_pack_parser.schemas.aspects_raw import ASPECT_TO_RAW_SCHEMA_MAP
import polars as pl

logger = logging.getLogger(__name__)


def load_mpk_files(directory_paths: List[str]) -> Dict[str, bytes]:
    """Loads raw .mpk files, handling duplicates and logging errors."""
    raw_files_content: Dict[str, bytes] = {}
    logger.info(f"Starting file ingestion from {directory_paths}")
    for dir_path in directory_paths:
        if not os.path.isdir(dir_path):
            logger.warning(f"Input directory not found or inaccessible: {dir_path}")
            continue
        for filename in os.listdir(dir_path):
            if filename.endswith(".mpk"):
                file_path = os.path.join(dir_path, filename)
                aspect_name = os.path.splitext(filename)[0]
                if aspect_name in raw_files_content:
                    logger.warning(
                        f"Duplicate aspect name '{aspect_name}' found. Overwriting previous file."
                    )
                try:
                    with open(file_path, "rb") as f:
                        raw_files_content[aspect_name] = f.read()
                except IOError as e:
                    logger.error(f"Failed to read file {file_path}: {e}")
    return raw_files_content


def load_unit_definitions(filepath: str) -> Dict[str, Any]:
    """
    Loads and parses the unit definitions from a specified JSON file.

    Args:
        filepath: The full path to the unitdefs.json file.

    Returns:
        A dictionary containing the parsed unit definition data.
    """
    logger.info(f"Loading unit definitions from: {filepath}")
    if not os.path.exists(filepath):
        raise FileIngestionError(f"Unit definitions file not found at path: {filepath}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        raise FileIngestionError(
            f"Failed to load or parse unit definitions JSON file: {e}"
        ) from e


def ingest_defs_csv(directory_paths: List[str]) -> Optional[pl.DataFrame]:
    """
    Scans input directories to find and load the first 'defs.csv' file encountered.

    Args:
        directory_paths: A list of directory paths to scan.

    Returns:
        A Polars DataFrame from the first found defs.csv, or None if not found.
    """
    logger.info(f"Scanning for defs.csv in: {directory_paths}")
    for dir_path in directory_paths:
        if not os.path.isdir(dir_path):
            continue  # Already warned by load_mpk_files

        defs_csv_path = os.path.join(dir_path, "defs.csv")
        if os.path.exists(defs_csv_path):
            logger.info(f"Discovered definitions map at: {defs_csv_path}")
            try:
                defs_map_df = (
                    pl.read_csv(
                        defs_csv_path,
                        has_header=True,
                        columns=["id", "name", "translatedHumanName"],
                    )
                    .rename({"id": "unit_def_id", "name": "unit_name", "translatedHumanName": "translated_human_name"})
                    .with_columns(pl.col("unit_def_id").cast(pl.Int64))
                )

                logger.info(
                    f"Loaded and processed {defs_map_df.height} unit definitions from CSV."
                )
                return defs_map_df  # Return the first one we find
            except Exception as e:
                # Raise an error if we find the file but can't parse it
                raise FileIngestionError(
                    f"Found defs.csv but failed to load or parse it: {e}"
                ) from e

    logger.warning(
        "No 'defs.csv' file was found in any input directory. Stats requiring ID mapping may fail."
    )
    return None


def ingest_game_meta(input_dirs: List[str]) -> Optional[bytes]:
    """
    Finds and reads game_meta.json from the list of input directories.
    Returns the first one found as raw bytes.
    """
    for directory in input_dirs:
        filepath = os.path.join(directory, "game_meta.json")
        if os.path.exists(filepath):
            try:
                with open(filepath, "rb") as f:
                    logger.info(f"Successfully ingested static asset: game_meta.json")
                    return f.read()
            except IOError as e:
                logger.warning(f"Could not read game_meta.json at {filepath}: {e}")
    logger.warning("Static asset 'game_meta.json' not found in any input directory.")
    return None


def list_recognized_aspects() -> List[str]:
    """Returns a sorted list of aspect names recognized by the current schemas."""
    return sorted(list(ASPECT_TO_RAW_SCHEMA_MAP.keys()))
