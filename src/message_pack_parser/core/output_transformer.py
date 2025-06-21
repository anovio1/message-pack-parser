# src/message_pack_parser/core/output_transformer.py

import logging
from typing import Dict, Tuple, Any
import polars as pl

from .exceptions import TransformationError
from message_pack_parser.config.dynamic_config_builder import OUTPUT_TRANSFORMATION_CONFIG

logger = logging.getLogger(__name__)

def _transform_single_df(df: pl.DataFrame, contract: Dict) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """
    Applies transformations by building a list of Polars lazy expressions and
    executing them in a single pass for maximum performance. Metadata is generated
    in lock-step with expression creation to ensure correctness.
    """
    output_metadata = {"columns": {}, "table": contract.get("table_options", {})}
    expressions_to_apply = []

    for col_name in df.columns:
        col_contract = contract.get("columns", {}).get(col_name)

        # If there's no contract for this column, pass it through untouched.
        if not col_contract:
            expressions_to_apply.append(pl.col(col_name))
            continue

        # If there is a contract, build the transformation expression and metadata.
        expression = pl.col(col_name)
        original_dtype = str(df[col_name].dtype)
        transform_meta = {}

        transform_type = col_contract.get("transform")
        if transform_type == "quantize":
            to_type_str = col_contract["to_type"]
            params = col_contract.get("params", {})
            
            # --- Symmetric (Dynamic) Quantization ---
            # This is our primary, data-dependent logic.
            # We must perform this one calculation eagerly to get the scale factor.
            if not isinstance(df[col_name].dtype, (pl.Float32, pl.Float64)):
                 raise TransformationError(f"Quantization requires a float column, but '{col_name}' is {original_dtype}.")
            
            abs_max = df[col_name].abs().max()
            if to_type_str.startswith("U"):
                target_max = 2 ** int(to_type_str[4:]) - 1
            else:
                target_max = 2 ** (int(to_type_str[3:]) - 1) - 1

            scale = abs_max / target_max if abs_max and abs_max > 0 else 1.0
            
            # Build the expression and the metadata together
            expression = (expression / scale).round(0)
            transform_meta = {"transform": "symmetric_quantize", "scale": scale}
        
        elif transform_type == "cast":
            transform_meta = {"transform": "cast"}

        # --- Chained Operations ---
        # The final type cast is always applied after any value transformations.
        final_dtype_str = col_contract.get("to_type")
        if not final_dtype_str:
             raise TransformationError(f"Transformation contract for '{col_name}' is missing required 'to_type' key.")
        
        expression = expression.cast(getattr(pl, final_dtype_str))
        
        # --- Finalize and store results for this column ---
        expressions_to_apply.append(expression.alias(col_name))
        
        # Store the complete, correct metadata
        transform_meta["original_dtype"] = original_dtype
        output_metadata["columns"][col_name] = transform_meta

    # Execute the entire plan in a single, optimized select statement.
    output_df = df.select(expressions_to_apply)

    return output_df, output_metadata


def apply_output_transformations(
    aggregated_stats: Dict[str, pl.DataFrame], unaggregated_df: pl.DataFrame
) -> Tuple[Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]], Tuple[pl.DataFrame, Dict[str, Any]]]:
    """Orchestrates the transformation of all data streams."""
    final_agg_results = {}
    for stat_name, df in aggregated_stats.items():
        contract = OUTPUT_TRANSFORMATION_CONFIG.get(stat_name, {})
        final_agg_results[stat_name] = _transform_single_df(df, contract)

    unagg_contract = OUTPUT_TRANSFORMATION_CONFIG.get("unaggregated", {})
    final_unagg_result = _transform_single_df(unaggregated_df, unagg_contract)

    return final_agg_results, final_unagg_result