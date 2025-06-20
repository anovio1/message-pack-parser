"""
This module is responsible for building the execution context for the parser,
including loading configurations and preparing initial DataFrames like unit_defs.
"""

import importlib.resources
import logging
from pathlib import Path
from typing import Optional, Dict, Any

import polars as pl

from message_pack_parser.core.ingestion import load_unit_definitions
from message_pack_parser.core.exceptions import ParserError
from message_pack_parser.schemas.unit_defs_schema import UnitDef

logger = logging.getLogger(__name__)


def build_execution_context(
    unit_defs_path: Optional[Path],
    defs_map_df: Optional[pl.DataFrame]
) -> Dict[str, pl.DataFrame]:
    """
    Builds the initial context, including loading and validating the unit_defs DataFrame.

    This function handles the logic of using a user-provided path or falling back
    to the packaged default.

    Args:
        unit_defs_path: An optional path to a custom unitdefs.json file.

    Returns:
        A dictionary to be used as the initial state for the 'dataframes' collection,
        containing the 'unit_defs' DataFrame.
    """
    logger.info("--- [Pre-Step] Building Execution Context ---")
    initial_context: Dict[str, pl.DataFrame] = {}

    final_unit_defs_path: Path
    if unit_defs_path:
        logger.info(f"Using custom unit definitions file from: {unit_defs_path}")
        final_unit_defs_path = unit_defs_path
    else:
        logger.info("No custom unit definitions file provided, using packaged default.")
        try:
            with importlib.resources.path("message_pack_parser.config.defaults", "unitdefs.json") as p:
                final_unit_defs_path = p
        except FileNotFoundError:
             raise ParserError("Default unitdefs.json not found in package. This indicates a bad installation.")
    
    unit_defs_json = load_unit_definitions(str(final_unit_defs_path))
    validated_defs = {}
    for name, data in unit_defs_json.items():
        model_instance = UnitDef.model_validate(data)
        model_instance.unit_name = name
        validated_defs[name] = model_instance
    unit_defs_df = pl.DataFrame([d.model_dump() for d in validated_defs.values()])
    logger.info(f"Loaded and validated {unit_defs_df.height} unit definitions.")
    initial_context["unit_defs"] = unit_defs_df
    
    # 2. Add the provided defs_map DataFrame to the context
    if defs_map_df is not None:
        initial_context["defs_map"] = defs_map_df
    else:
        # Create an empty placeholder if no file was found
        initial_context["defs_map"] = pl.DataFrame(schema={"unit_def_id": pl.Int64, "unit_name": pl.Utf8})

    return initial_context