# src/message_pack_parser/core/output_transformer.py

import copy
import logging
from typing import Dict, Tuple, Any
import polars as pl

from message_pack_parser.config.enums import ENUM_REGISTRY
from .exceptions import TransformationError
from message_pack_parser.config.dynamic_config_builder import (
    OUTPUT_TRANSFORMATION_CONFIG,
)

logger = logging.getLogger(__name__)


def _transform_single_df(
    df: pl.DataFrame, contract: Dict
) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """
    Applies transformations by building a list of Polars lazy expressions and
    executing them in a single pass for maximum performance.
    The returned metadata preserves **every** column-level option that appeared
    in the contract (e.g. `null_encoding`) and adds runtime details such as the
    source dtype.
    """
    output_metadata: Dict[str, Any] = {
        "columns": {},
        "table": contract.get("table_options", {}),
    }
    expressions_to_apply = []

    for col_name in df.columns:
        col_contract: Dict[str, Any] = contract.get("columns", {}).get(col_name)

        # If there's no contract for this column, pass it through untouched.
        if not col_contract:
            expressions_to_apply.append(pl.col(col_name))
            output_metadata["columns"][col_name] = {
                "transform": "none",
                "original_dtype": str(df[col_name].dtype),
            }
            continue

        # Start with a deep copy of the contract so *all* keys survive
        transform_meta: Dict[str, Any] = copy.deepcopy(col_contract)
        original_dtype = str(df[col_name].dtype)

        # If there is a contract, build the transformation expression and metadata.
        expression = pl.col(col_name)

        transform_type = col_contract.get("transform")

        if transform_type == "enum_to_int":
            params = col_contract.get("params", {})
            enum_key = params.get("enum_key")
            if not enum_key:
                raise TransformationError(
                    f"enum_to_int for '{col_name}' requires 'enum_key' in params."
                )

            enum_class = ENUM_REGISTRY.get(enum_key)
            if not enum_class:
                raise TransformationError(
                    f"Enum key '{enum_key}' not found in ENUM_REGISTRY."
                )

            # Replace ambiguous map_dict with an explicit when/then chain.
            # This is the most robust way to do conditional replacement.
            # Start with a null default case.
            mapping_expr = pl.when(False).then(None)
            for member in enum_class:
                # For each enum member, add a condition.
                mapping_expr = mapping_expr.when(pl.col(col_name) == member.name).then(
                    pl.lit(member.value)
                )

            # The final expression is the full chain.
            expression = mapping_expr

            transform_meta.update(
                {
                    "transform": "enum_to_int",
                    "enum_map": {member.value: member.name for member in enum_class},
                }
            )

        # QUANTIZE
        elif transform_type == "quantize":
            params = col_contract.get("params", {})
            if params.get("type") == "static":
                scale = params["scale"]
                expression = (expression.cast(pl.Float64) / scale).round(0)
                transform_meta.update({"transform": "static_quantize", "scale": scale})

        # SIMPLE CAST
        elif transform_type == "cast":
            transform_meta.setdefault("transform", "cast")

        # --- Chained Operations ---
        # The final type cast is always applied after any value transformations.
        final_dtype_str = col_contract.get("to_type")
        if not final_dtype_str:
            raise TransformationError(
                f"Transformation contract for '{col_name}' is missing required 'to_type'."
            )
            
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
    aggregated_stats: Dict[str, pl.DataFrame],
    unaggregated_streams: Dict[str, pl.DataFrame],
) -> Tuple[Dict[str, Tuple[pl.DataFrame, Dict]], Dict[str, Tuple[pl.DataFrame, Dict]]]:
    """Orchestrates the transformation of all data streams."""
    final_agg_results = {}
    for stat_name, df in aggregated_stats.items():
        agg_contract = OUTPUT_TRANSFORMATION_CONFIG.get(stat_name, {})
        final_agg_results[stat_name] = _transform_single_df(df, agg_contract)

    final_unagg_results = {}
    for stream_name, df in unaggregated_streams.items():
        unagg_contract = OUTPUT_TRANSFORMATION_CONFIG.get(stream_name, {})
        final_unagg_results[stream_name] = _transform_single_df(df, unagg_contract)

    return final_agg_results, final_unagg_results
