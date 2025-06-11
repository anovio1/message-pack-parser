"""Step 7: Final Output Generation"""
from typing import Dict, Any, List, Optional
import polars as pl; import msgpack; import gzip; import os; import logging
from message_pack_parser.core.exceptions import OutputGenerationError

logger = logging.getLogger(__name__)

def _generate_output_map(agg_cols: Optional[List[str]] = None, unagg_cols: Optional[List[str]] = None) -> Dict[str, Any]:
    return {"version": "3.0", "description": "Master data file", "data_streams": {"aggregated": {"columns": agg_cols or []}, "unaggregated": {"columns": unagg_cols or []}}}

def generate_final_output(agg_df: pl.DataFrame, unagg_df: pl.DataFrame, output_dir: str, replay_id: str):
    logger.info("Starting Step 7: Final Output Generation")
    os.makedirs(output_dir, exist_ok=True)
    output_filepath = os.path.join(output_dir, f"{replay_id}_master_data.mpk.gz")

    master_obj = {
        "replay_id": replay_id,
        "map": _generate_output_map(agg_df.columns if agg_df is not None else [], unagg_df.columns if unagg_df is not None else []),
        "data": {
            "aggregated": agg_df.to_dicts() if agg_df is not None and not agg_df.is_empty() else [],
            "unaggregated": unagg_df.to_dicts() if unagg_df is not None and not unagg_df.is_empty() else [],
        }
    }
    try:
        packed = msgpack.packb(master_obj, use_bin_type=True)
        with gzip.open(output_filepath, "wb") as f: f.write(packed)
        logger.info(f"Final output saved to: {output_filepath}")
    except (TypeError, IOError) as e:
        raise OutputGenerationError(f"Failed to serialize or write final output for {replay_id}") from e