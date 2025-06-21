# src/message_pack_parser/core/output_transformer.py
"""
Output Transformation

This module takes analytically-ready DataFrames/Polars from the aggregator and
transforms them, using a config from schemas/output_contracts.py, for specific
downstream consumers
"""
import logging
from typing import Dict, Tuple, Any

import polars as pl

from .exceptions import TransformationError
from message_pack_parser.config.dynamic_config_builder import (
    OUTPUT_TRANSFORMATION_CONFIG,
)

logger = logging.getLogger(__name__)


def _apply_quantization(
    series: pl.Series, col_contract: Dict
) -> Tuple[pl.Series, Dict[str, Any]]:
    """
    Applies quantization to a series based on its contract.
    Supports both dynamic (data-driven) and static (predefined) quantization.
    """
    original_dtype = str(series.dtype)
    to_type_str = col_contract["to_type"]
    params = col_contract.get("params", {})

    if params.get("type") == "static":
        # --- Static Quantization Logic ---
        scale = params.get("scale")
        if scale is None:
            raise TransformationError(
                f"Static quantization contract for column '{series.name}' requires a 'scale' parameter."
            )

        quantized_series = (series / scale).round(0).cast(getattr(pl, to_type_str))

        metadata = {
            "transform": "static_quantize",
            "scale": scale,
            "original_dtype": original_dtype,
        }

    else:
        # --- Dynamic Symmetric Quantization Logic ---
        if not isinstance(series.dtype, (pl.Float32, pl.Float64)):
            raise TransformationError(
                f"Dynamic quantization can only be applied to float types, not {original_dtype} for column '{series.name}'."
            )

        abs_max = series.abs().max()
        if to_type_str.startswith("U"):
            target_max = 2 ** int(to_type_str[4:]) - 1
        else:
            target_max = 2 ** (int(to_type_str[3:]) - 1) - 1

        if abs_max is None or abs_max == 0:
            scale = 1.0
        else:
            scale = abs_max / target_max

        quantized_series = (series / scale).round(0).cast(getattr(pl, to_type_str))

        metadata = {
            "transform": "symmetric_quantize",
            "scale": scale,
            "original_dtype": original_dtype,
        }

    return quantized_series, metadata


def _transform_single_df(
    df: pl.DataFrame, contract: Dict
) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """Helper to apply transformations to a single DataFrame based on its contract."""
    output_df = df.clone()
    output_metadata = {
        "columns": {},
        # Pass table-level options from the contract into the final metadata
        "table": contract.get("table_options", {}),
    }

    for col_name, col_contract in contract.get("columns", {}).items():
        try:
            transform_type = col_contract.get("transform")

            if transform_type == "quantize":
                original_series = output_df.get_column(col_name)
                new_series, transform_meta = _apply_quantization(
                    original_series, col_contract
                )
                output_df = output_df.with_columns(new_series.alias(col_name))
                output_metadata["columns"][col_name] = transform_meta

            elif transform_type == "cast":
                to_type_str = col_contract["to_type"]
                # Create the new series with the original name
                new_series = output_df.get_column(col_name).cast(
                    getattr(pl, to_type_str)
                )
                # Overwrite the old column with the new casted one
                output_df = output_df.with_columns(new_series.alias(col_name))
                # No special transform metadata is needed for a simple cast.
                output_metadata["columns"][col_name] = {
                    "transform": "none",
                    "original_dtype": str(df.get_column(col_name).dtype),
                }

        except pl.ColumnNotFoundError:
            logger.warning(
                f"Contract specified transformation for column '{col_name}', but it was not found. Skipping."
            )
            continue
        except Exception as e:
            logger.error(
                f"Failed to apply transformation for column '{col_name}'. Error: {e}",
                exc_info=True,
            )
            raise TransformationError(
                f"Transformation failed for column '{col_name}'"
            ) from e

    return output_df, output_metadata


def apply_output_transformations(
    aggregated_stats: Dict[str, pl.DataFrame], unaggregated_df: pl.DataFrame
) -> Tuple[
    Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]], Tuple[pl.DataFrame, Dict[str, Any]]
]:
    """
    Orchestrates the transformation of all aggregated and unaggregated data streams.
    This is the entry point for pipeline Step 6.5.
    """
    final_agg_results = {}
    for stat_name, df in aggregated_stats.items():
        contract = OUTPUT_TRANSFORMATION_CONFIG.get(stat_name, {})
        final_agg_results[stat_name] = _transform_single_df(df, contract)

    unagg_contract = OUTPUT_TRANSFORMATION_CONFIG.get("unaggregated", {})
    final_unagg_result = _transform_single_df(unaggregated_df, unagg_contract)

    return final_agg_results, final_unagg_result
