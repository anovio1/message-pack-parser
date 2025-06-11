import os
import logging
from typing import List, Dict
from message_pack_parser.schemas.aspects_raw import ASPECT_TO_RAW_SCHEMA_MAP

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
                    logger.warning(f"Duplicate aspect name '{aspect_name}' found. Overwriting previous file.")
                try:
                    with open(file_path, "rb") as f:
                        raw_files_content[aspect_name] = f.read()
                except IOError as e:
                    logger.error(f"Failed to read file {file_path}: {e}")
    return raw_files_content

def list_recognized_aspects() -> List[str]:
    """Returns a sorted list of aspect names recognized by the current schemas."""
    return sorted(list(ASPECT_TO_RAW_SCHEMA_MAP.keys()))