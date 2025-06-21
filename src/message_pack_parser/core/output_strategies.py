# src/message_pack_parser/core/output_strategies.py
"""
Module defining different strategies for writing the final output data.
"""
import datetime
import io
import json
import os

from abc import ABC, abstractmethod
from enum import Enum
from datetime import datetime, timezone
import struct
from typing import Dict, Tuple, Type, Any, List

import polars as pl
import msgpack
import gzip
import zstandard as zstd

import logging

from message_pack_parser.core.exceptions import OutputGenerationError

logger = logging.getLogger(__name__)


# Enum for CLI choices
class OutputFormat(str, Enum):
    MPK_GZIP = "mpk-gzip"
    PARQUET_DIR = "parquet-dir"
    JSONL_GZIP = "jsonl-gzip"
    COL_ZST = "col-zst"
    ROW_ZST = "row-zst"


class OutputStrategy(ABC):
    """Abstract base class for all output writing strategies."""

    @abstractmethod
    def write(
        self,
        transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        transformed_unaggregated_data: Tuple[pl.DataFrame, Dict[str, Any]],
        output_directory: str,
        replay_id: str,
    ) -> None:
        """
        Writes the provided DataFrames to a specified location using a specific format.

        Args:
            transformed_aggregated_data: A dictionary mapping stat names to their aggregated DataFrame.
            transformed_unaggregated_data: The DataFrame containing detailed, unaggregated data.
            output_directory: The base directory where output should be saved.
            replay_id: The unique identifier for the replay.
        """
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Executing output strategy: {self.__class__.__name__}")


def _get_struct_format_string(dtypes: list[pl.DataType]) -> str:
    """
    Maps a list of Polars dtypes to a Python struct format string.
    See: https://docs.python.org/3/library/struct.html#format-characters
    """
    format_char_map = {
        pl.Int8: "b",
        pl.UInt8: "B",
        pl.Int16: "h",
        pl.UInt16: "H",
        pl.Int32: "i",
        pl.UInt32: "I",
        pl.Int64: "q",
        pl.UInt64: "Q",
        pl.Float32: "f",
        pl.Float64: "d",
    }
    format_chars = ["<"]  # Use little-endian for cross-platform consistency
    for dtype in dtypes:
        char = format_char_map.get(dtype)
        if char is None:
            raise TypeError(
                f"Cannot create struct format for unsupported Polars dtype: {dtype}"
            )
        format_chars.append(char)

    return "".join(format_chars)


class ColumnarBundleZstStrategy(OutputStrategy):
    """
    Creates one zstd-compressed binary file per table, using a column-major
    (Struct of Arrays) layout. A master schema.json file describes the layout.
    This is the most flexible and performant format for analytics.
    """

    def write(
        self,
        transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        transformed_unaggregated_data: Tuple[pl.DataFrame, Dict[str, Any]],
        output_directory: str,
        replay_id: str,
    ) -> None:

        super().write(
            transformed_aggregated_data,
            transformed_unaggregated_data,
            output_directory,
            replay_id,
        )
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)
        schema = {
            "replay_id": replay_id,
            "schema_version": "5.0-columnar",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "streams": {},
        }
        all_streams = transformed_aggregated_data.copy()
        all_streams["unaggregated"] = transformed_unaggregated_data

        for stream_name, (df, metadata) in all_streams.items():
            if df.is_empty():
                continue

            payload_buffer = io.BytesIO()
            stream_column_metadata = []
            current_offset = 0

            for series in df:
                column_bytes = series.to_numpy().tobytes()
                payload_buffer.write(column_bytes)

                col_length = len(column_bytes)
                transform_info = metadata.get("columns", {}).get(
                    series.name, {"transform": "none"}
                )
                stream_column_metadata.append(
                    {
                        "name": series.name,
                        "dtype": str(series.dtype),
                        "offset": current_offset,
                        "length": col_length,
                        "original_dtype": transform_info.pop(
                            "original_dtype", str(series.dtype)
                        ),
                        "transform": transform_info,
                    }
                )
                current_offset += col_length
            try:
                compressed_payload = zstd.ZstdCompressor().compress(
                    payload_buffer.getvalue()
                )
                filename = f"{stream_name}.bin.zst"
                output_path = os.path.join(replay_output_dir, filename)
                with open(output_path, "wb") as f_out:
                    f_out.write(compressed_payload)
            except Exception as e:
                raise OutputGenerationError(
                    f"Failed to write columnar bundle for {stream_name}"
                ) from e
            schema["streams"][stream_name] = {
                "num_rows": len(df),
                "file": filename,
                "columns": stream_column_metadata,
            }

        try:
            schema_path = os.path.join(replay_output_dir, "schema.json")
            with open(schema_path, "w") as f_schema:
                json.dump(schema, f_schema, indent=2)
        except IOError as e:
            raise OutputGenerationError("Failed to write final schema.json file") from e
        logger.info(
            f"Successfully wrote columnar binary bundles and schema.json to {replay_output_dir}"
        )


class RowMajorBundleZstStrategy(OutputStrategy):
    """
    Creates one zstd-compressed binary file per table, using a row-major
    (Array of Structs) layout. This is designed for consumers that process data
    row-by-row and requires that all columns be cast to a uniform dtype.
    """

    def write(
        self,
        transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        transformed_unaggregated_data: Tuple[pl.DataFrame, Dict[str, Any]],
        output_directory: str,
        replay_id: str,
    ) -> None:
        super().write(
            transformed_aggregated_data,
            transformed_unaggregated_data,
            output_directory,
            replay_id,
        )
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)

        schema = {
            "replay_id": replay_id,
            "schema_version": "7.0-row-major-mixed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "streams": {},
        }
        all_streams = transformed_aggregated_data.copy()
        all_streams["unaggregated"] = transformed_unaggregated_data

        for stream_name, (df, metadata) in all_streams.items():
            print(stream_name)
            print(metadata)
            if df.is_empty():
                continue

            # Check the contract to see if this stream should be processed by this strategy
            table_options = metadata.get("table", {})
            if table_options.get("layout") != "row-major-mixed":
                logger.debug(
                    f"Stream '{stream_name}' is not marked for 'row-major-mixed' layout. Skipping."
                )
                continue

            try:
                format_string = _get_struct_format_string(df.dtypes)
                packer = struct.Struct(format_string)
            except TypeError as e:
                logger.warning(
                    f"Could not process stream '{stream_name}' for row-major output: {e}. Skipping."
                )
                continue

            with io.BytesIO() as buffer:
                for row in df.iter_rows():
                    buffer.write(packer.pack(*row))
                packed_bytes = buffer.getvalue()

            compressed_payload = zstd.ZstdCompressor().compress(packed_bytes)

            filename = f"{stream_name}.rows.bin.zst"
            output_path = os.path.join(replay_output_dir, filename)
            with open(output_path, "wb") as f_out:
                f_out.write(compressed_payload)

            schema["streams"][stream_name] = {
                "num_rows": len(df),
                "row_byte_stride": packer.size,
                "file": filename,
                "layout": "row-major-mixed",
                "columns": [
                    {
                        "name": name,
                        "dtype": str(dtype),
                        "transform": metadata.get("columns", {}).get(
                            name, {"transform": "none"}
                        ),
                    }
                    for name, dtype in df.schema.items()
                ],
            }

        schema_path = os.path.join(replay_output_dir, "schema.json")
        with open(schema_path, "w") as f_schema:
            json.dump(schema, f_schema, indent=2)
        logger.info(
            f"Successfully wrote mixed-type row-major bundles and schema.json to {replay_output_dir}"
        )


class MessagePackGzipStrategy(OutputStrategy):
    """Writes a single, gzipped MessagePack file where the 'transformed_aggregated_data' key contains a nested dictionary of all computed stats."""

    def _generate_map(
        self, agg_stats: Dict[str, pl.DataFrame], unagg_cols: List[str]
    ) -> Dict[str, Any]:
        agg_schemas = {
            stat_name: {"columns": df.columns} for stat_name, df in agg_stats.items()
        }
        return {
            "version": "3.1",
            "description": "Master data file containing multiple aggregated stat streams.",
            "data_streams": {
                "aggregated_stats": agg_schemas,
                "unaggregated": {"columns": unagg_cols or []},
            },
        }

    def write(
        self,
        transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        transformed_unaggregated_data: Tuple[pl.DataFrame, Dict[str, Any]],
        output_directory: str,
        replay_id: str,
    ) -> None:
        super().write(
            transformed_aggregated_data,
            transformed_unaggregated_data,
            output_directory,
            replay_id,
        )

        agg_dfs = {name: df for name, (df, _) in transformed_aggregated_data.items()}
        unagg_df, _ = transformed_unaggregated_data

        output_filepath = os.path.join(output_directory, f"{replay_id}_master.mpk.gz")
        agg_data_payload = {
            stat_name: df.to_dicts() for stat_name, df in agg_dfs.items()
        }
        unagg_data = unagg_df.to_dicts() if not unagg_df.is_empty() else []
        master_object: Dict[str, Any] = {
            "replay_id": replay_id,
            "map": self._generate_map(agg_dfs, unagg_df.columns),
            "data": {
                "transformed_aggregated_data": agg_data_payload,
                "unaggregated": unagg_data,
            },
        }
        try:
            packed_data = msgpack.packb(master_object, use_bin_type=True)
            assert isinstance(packed_data, bytes)
            with gzip.open(output_filepath, "wb") as f:
                f.write(packed_data)
            logger.info(
                f"Successfully wrote MessagePack/Gzip output to: {output_filepath}"
            )
        except (TypeError, IOError) as e:
            raise OutputGenerationError(
                f"Failed to write MessagePack/Gzip output for {replay_id}"
            ) from e


class ParquetDirectoryStrategy(OutputStrategy):
    """
    Writes each computed aggregated stat to its own named .parquet file
    inside a dedicated directory for the replay.
    """

    def write(
        self,
        transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        transformed_unaggregated_data: Tuple[pl.DataFrame, Dict[str, Any]],
        output_directory: str,
        replay_id: str,
    ) -> None:
        super().write(
            transformed_aggregated_data,
            transformed_unaggregated_data,
            output_directory,
            replay_id,
        )

        agg_dfs = {name: df for name, (df, _) in transformed_aggregated_data.items()}
        unagg_df, _ = transformed_unaggregated_data

        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)

        try:
            for stat_name, df in agg_dfs.items():
                stat_path = os.path.join(replay_output_dir, f"{stat_name}.parquet")
                df.write_parquet(stat_path)
                logger.info(f"Successfully wrote stat '{stat_name}' to: {stat_path}")
            if not unagg_df.is_empty():
                unagg_path = os.path.join(replay_output_dir, "unaggregated.parquet")
                unagg_df.write_parquet(unagg_path)
                logger.info(
                    f"Successfully wrote unaggregated Parquet data to: {unagg_path}"
                )
        except Exception as e:
            raise OutputGenerationError(
                f"Failed to write Parquet output for {replay_id}"
            ) from e


class JsonLinesGzipStrategy(OutputStrategy):
    """
    Writes each aggregated stat and the unaggregated stream to a separate .jsonl.gz file.
    """

    def _write_single_stream(self, df: pl.DataFrame, output_path: str):
        """Helper to serialize a single DataFrame to a gzipped NDJSON file."""
        if df.is_empty():
            logger.warning(f"DataFrame is empty, skipping write to {output_path}")
            return

        ndjson_string = df.write_ndjson()
        try:
            with gzip.open(output_path, "wb") as f:
                f.write(ndjson_string.encode("utf-8"))
            logger.info(
                f"Successfully wrote gzipped JSON Lines output to: {output_path}"
            )
        except IOError as e:
            raise OutputGenerationError(
                f"Failed to write gzipped file to {output_path}"
            ) from e

    def write(
        self,
        transformed_aggregated_data: Dict[str, Tuple[pl.DataFrame, Dict[str, Any]]],
        transformed_unaggregated_data: Tuple[pl.DataFrame, Dict[str, Any]],
        output_directory: str,
        replay_id: str,
    ) -> None:
        super().write(
            transformed_aggregated_data,
            transformed_unaggregated_data,
            output_directory,
            replay_id,
        )

        agg_dfs = {name: df for name, (df, _) in transformed_aggregated_data.items()}
        unagg_df, _ = transformed_unaggregated_data

        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)

        try:
            # 1. Loop through the dictionary of computed aggregated stats.
            for stat_name, df in agg_dfs.items():
                stat_path = os.path.join(
                    output_directory, f"{replay_id}_{stat_name}.jsonl.gz"
                )
                # 2. Pass the actual DataFrame (not the dictionary) to the helper.
                self._write_single_stream(df, stat_path)

            # 3. Handle the unaggregated stream as before.
            if not unagg_df.is_empty():
                unagg_path = os.path.join(
                    output_directory, f"{replay_id}_unaggregated.jsonl.gz"
                )
                self._write_single_stream(unagg_df, unagg_path)
        except Exception as e:
            raise OutputGenerationError(
                f"Failed to write JSON Lines output for {replay_id}"
            ) from e


STRATEGY_MAP: Dict[OutputFormat, Type[OutputStrategy]] = {
    OutputFormat.MPK_GZIP: MessagePackGzipStrategy,
    OutputFormat.PARQUET_DIR: ParquetDirectoryStrategy,
    OutputFormat.JSONL_GZIP: JsonLinesGzipStrategy,
    OutputFormat.COL_ZST: ColumnarBundleZstStrategy,
    OutputFormat.ROW_ZST: RowMajorBundleZstStrategy,
}
