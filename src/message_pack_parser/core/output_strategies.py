# src/message_pack_parser/core/output_strategies.py
"""
Module defining different strategies for writing the final output data using the
Template Method design pattern for a clean, extensible architecture.
"""
import io
import json
import os
import struct
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Tuple, Type, Any
from datetime import datetime, timezone

import polars as pl
import zstandard as zstd
import msgpack
import gzip
import logging

from message_pack_parser.core.exceptions import OutputGenerationError

logger = logging.getLogger(__name__)


class OutputFormat(str, Enum):
    HYBRID_MPK_ZST = "hybrid-mpk-zst"
    ROW_MAJOR_ZST = "row-major-zst"
    COLUMNAR_ZST = "columnar-zst"
    PARQUET_DIR = "parquet-dir"
    JSONL_GZIP = "jsonl-gzip"
    MPK_GZIP = "mpk-gzip"

def _get_struct_format_string(dtypes: list[pl.DataType]) -> str:
    format_char_map = {
        pl.Int8: 'b', pl.UInt8: 'B', pl.Int16: 'h', pl.UInt16: 'H',
        pl.Int32: 'i', pl.UInt32: 'I', pl.Int64: 'q', pl.UInt64: 'Q',
        pl.Float32: 'f', pl.Float64: 'd',
    }
    format_chars = ['<']
    for dtype in dtypes:
        char = format_char_map.get(dtype)
        if char is None: raise TypeError(f"Unsupported dtype for struct packing: {dtype}")
        format_chars.append(char)
    return ''.join(format_chars)


class OutputStrategy(ABC):
    """Abstract base class using the Template Method design pattern."""
    
    def write(
        self,
        transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        transformed_unaggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        output_directory: str,
        replay_id: str,
    ) -> None:
        """Public template method that orchestrates the writing process."""
        logger.info(f"Executing output strategy: {self.__class__.__name__}")
        all_streams = transformed_aggregated_data.copy()
        all_streams.update(transformed_unaggregated_data)
        
        try:
            # All common setup is done here. Subclasses just need to write.
            self._execute_write(all_streams, output_directory, replay_id)
        except Exception as e:
            # Centralized error wrapping for all strategies.
            raise OutputGenerationError(f"Failed to execute strategy {self.__class__.__name__}") from e

    @abstractmethod
    def _execute_write(
        self,
        all_streams: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        output_directory: str,
        replay_id: str
    ) -> None:
        """
        Protected abstract method for subclasses to implement their specific
        serialization logic.
        """
        pass


# --- HIGH-PERFORMANCE BINARY STRATEGIES ---

class HybridMessagePackZstStrategy(OutputStrategy):
    """Creates a single, self-contained .mpk.zst file."""
    
    def _get_column_schema(self, series_name: str, series_dtype: str, data_key: str, metadata: Dict) -> Dict:
        transform_info = metadata.get("columns", {}).get(series_name, {}).copy()
        original_dtype = transform_info.pop("original_dtype", series_dtype)
        return {"name": series_name, "dtype": series_dtype, "data_key": data_key, "original_dtype": original_dtype, "transform": transform_info}

    def _build_payloads(self, all_streams: Dict) -> Tuple[Dict, Dict]:
        streams_schema, data_blobs = {}, {}
        for stream_name, (df, metadata) in all_streams.items():
            if df.is_empty(): continue
            table_options = metadata.get("table", {})
            layout = table_options.get("layout", "columnar")
            if layout == "row-major-mixed":
                try:
                    format_string = _get_struct_format_string(df.dtypes)
                    packer = struct.Struct(format_string)
                    with io.BytesIO() as buffer:
                        for row in df.iter_rows(): buffer.write(packer.pack(*row))
                        data_blobs[stream_name] = buffer.getvalue()
                    
                    row_major_cols_schema = [self._get_column_schema(n, str(d), stream_name, metadata) for n, d in df.schema.items()]
                    streams_schema[stream_name] = {"layout": "row-major-mixed", "num_rows": len(df), "row_byte_stride": packer.size, "data_key": stream_name, "columns": row_major_cols_schema}
                except TypeError as e: logger.warning(f"Skipping row-major for '{stream_name}': {e}")
            else:
                stream_cols_schema = []
                for series in df:
                    data_key = f"{stream_name}_{series.name}"
                    data_blobs[data_key] = series.to_numpy().tobytes()
                    stream_cols_schema.append(self._get_column_schema(series.name, str(series.dtype), data_key, metadata))
                streams_schema[stream_name] = {"layout": "columnar", "num_rows": len(df), "columns": stream_cols_schema}
        return streams_schema, data_blobs

    def _execute_write(self, all_streams, output_directory, replay_id) -> None:
        os.makedirs(output_directory, exist_ok=True)
        streams_schema, data_payloads = self._build_payloads(all_streams)
        master_object = {
            "schema": {
                "replay_id": replay_id, "schema_version": "8.2-hybrid-mpk",
                "generated_at": datetime.now(timezone.utc).isoformat(), "streams": streams_schema,
            },
            "data": data_payloads
        }
        output_filepath = os.path.join(output_directory, f"{replay_id}.mpk.zst")
        packed_data = msgpack.packb(master_object, use_bin_type=True)
        assert(isinstance(packed_data, bytes))
        compressed_data = zstd.ZstdCompressor().compress(packed_data)
        with open(output_filepath, "wb") as f: f.write(compressed_data)
        logger.info(f"Successfully wrote hybrid MessagePack bundle to: {output_filepath}")

class RowMajorBundleZstStrategy(OutputStrategy):
    """Creates a schema.json and one zstd-compressed binary file per row-major table."""
    def _execute_write(self, all_streams, output_directory, replay_id) -> None:
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)
        schema = {
            "replay_id": replay_id, "schema_version": "7.0-row-major-mixed",
            "generated_at": datetime.now(timezone.utc).isoformat(), "streams": {},
        }
        for stream_name, (df, metadata) in all_streams.items():
            if df.is_empty() or metadata.get("table", {}).get("layout") != "row-major-mixed": continue
            try:
                format_string = _get_struct_format_string(df.dtypes)
                packer = struct.Struct(format_string)
                with io.BytesIO() as buffer:
                    for row in df.iter_rows(): buffer.write(packer.pack(*row))
                    packed_bytes = buffer.getvalue()
                compressed_payload = zstd.ZstdCompressor().compress(packed_bytes)
                filename = f"{stream_name}.rows.bin.zst"
                output_path = os.path.join(replay_output_dir, filename)
                with open(output_path, "wb") as f_out: f_out.write(compressed_payload)
                schema["streams"][stream_name] = {"num_rows": len(df), "row_byte_stride": packer.size, "file": filename, "layout": "row-major-mixed", "columns": [{"name": n, "dtype": str(d), **metadata.get("columns", {}).get(n, {})} for n, d in df.schema.items()]}
            except TypeError as e: logger.warning(f"Could not process stream '{stream_name}' for row-major output: {e}. Skipping.")
        schema_path = os.path.join(replay_output_dir, "schema.json")
        with open(schema_path, "w") as f_schema: json.dump(schema, f_schema, indent=2)
        logger.info(f"Successfully wrote mixed-type row-major bundles and schema.json to {replay_output_dir}")

class ColumnarBundleZstStrategy(OutputStrategy):
    """Creates a schema.json and one zstd-compressed binary file per column."""
    def _execute_write(self, all_streams, output_directory, replay_id) -> None:
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)
        schema = {
            "replay_id": replay_id, "schema_version": "6.0-columnar",
            "generated_at": datetime.now(timezone.utc).isoformat(), "streams": {},
        }
        for stream_name, (df, metadata) in all_streams.items():
            if df.is_empty() or metadata.get("table", {}).get("layout") == "row-major-mixed": continue
            stream_cols_schema = []
            for series in df:
                filename = f"{stream_name}_{series.name}.bin.zst"
                output_path = os.path.join(replay_output_dir, filename)
                column_bytes = series.to_numpy().tobytes()
                compressed_payload = zstd.ZstdCompressor().compress(column_bytes)
                with open(output_path, "wb") as f_out: f_out.write(compressed_payload)
                stream_cols_schema.append({"name": series.name, "dtype": str(series.dtype), "file": filename, **metadata.get("columns", {}).get(series.name, {})})
            schema["streams"][stream_name] = {"layout": "columnar", "num_rows": len(df), "columns": stream_cols_schema}
        schema_path = os.path.join(replay_output_dir, "schema.json")
        with open(schema_path, "w") as f_schema: json.dump(schema, f_schema, indent=2)
        logger.info(f"Successfully wrote columnar binary files and schema.json to {replay_output_dir}")


# --- LEGACY / UTILITY STRATEGIES (Refactored) ---

class ParquetDirectoryStrategy(OutputStrategy):
    """Writes each data stream to its own .parquet file."""
    def _execute_write(self, all_streams, output_directory, replay_id) -> None:
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)
        for stream_name, (df, _) in all_streams.items():
            if df.is_empty(): continue
            stat_path = os.path.join(replay_output_dir, f"{stream_name}.parquet")
            df.write_parquet(stat_path)
            logger.info(f"Successfully wrote stat '{stream_name}' to: {stat_path}")

class JsonLinesGzipStrategy(OutputStrategy):
    """Writes each data stream to its own .jsonl.gz file."""
    def _write_single_stream(self, df: pl.DataFrame, output_path: str):
        if df.is_empty():
            logger.warning(f"DataFrame is empty, skipping write to {output_path}")
            return
        # Use 'wt' text mode and allow gzip to handle encoding. Stream directly to file.
        with gzip.open(output_path, 'wt', encoding='utf-8') as f_gz:
            df.write_ndjson(f_gz)
        logger.info(f"Successfully wrote gzipped JSON Lines output to: {output_path}")

    def _execute_write(self, all_streams, output_directory, replay_id) -> None:
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)
        for stream_name, (df, _) in all_streams.items():
            stat_path = os.path.join(replay_output_dir, f"{stream_name}.jsonl.gz")
            self._write_single_stream(df, stat_path)

class MessagePackGzipStrategy(OutputStrategy):
    """Writes a single, gzipped MessagePack file (legacy format)."""
    def _execute_write(self, all_streams, output_directory, replay_id) -> None:
        os.makedirs(output_directory, exist_ok=True)
        # We ignore metadata for this legacy format
        payload = {name: df.to_dicts() for name, (df, _) in all_streams.items()}
        master_object: Dict[str, Any] = {"replay_id": replay_id, "data": payload}
        
        output_filepath = os.path.join(output_directory, f"{replay_id}_master.mpk.gz")
        packed_data = msgpack.packb(master_object, use_bin_type=True)
        assert(isinstance(packed_data, bytes))
        with gzip.open(output_filepath, "wb") as f: f.write(packed_data)
        logger.info(f"Successfully wrote legacy MessagePack/Gzip output to: {output_filepath}")


# --- FINAL STRATEGY MAP ---
STRATEGY_MAP: Dict[OutputFormat, Type[OutputStrategy]] = {
    OutputFormat.HYBRID_MPK_ZST: HybridMessagePackZstStrategy,
    OutputFormat.ROW_MAJOR_ZST: RowMajorBundleZstStrategy,
    OutputFormat.COLUMNAR_ZST: ColumnarBundleZstStrategy,
    OutputFormat.PARQUET_DIR: ParquetDirectoryStrategy,
    OutputFormat.JSONL_GZIP: JsonLinesGzipStrategy,
    OutputFormat.MPK_GZIP: MessagePackGzipStrategy,
}