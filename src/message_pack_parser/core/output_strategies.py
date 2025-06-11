"""
Module defining different strategies for writing the final output data.
This implements the Strategy design pattern for output generation.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import os
import polars as pl
import msgpack
import gzip
import logging

from message_pack_parser.core.exceptions import OutputGenerationError

logger = logging.getLogger(__name__)

class OutputStrategy(ABC):
    """Abstract base class for all output writing strategies."""

    @abstractmethod
    def write(
        self,
        aggregated_df: pl.DataFrame,
        unaggregated_df: pl.DataFrame,
        output_directory: str,
        replay_id: str
    ) -> None:
        """
        Writes the provided DataFrames to a specified location using a specific format.
        """
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Executing output strategy: {self.__class__.__name__}")


class MessagePackGzipStrategy(OutputStrategy):
    """
    Writes a single, gzipped MessagePack file containing both data streams and a map.
    """
    # ... (This class is unchanged and correct) ...
    def _generate_map(self, agg_cols: List[str], unagg_cols: List[str]) -> Dict[str, Any]:
        return {
            "version": "3.0", "description": "Master data file containing aggregated and unaggregated streams.",
            "data_streams": {"aggregated": {"columns": agg_cols or []}, "unaggregated": {"columns": unagg_cols or []}}
        }
    def write(self, aggregated_df: pl.DataFrame, unaggregated_df: pl.DataFrame, output_directory: str, replay_id: str) -> None:
        super().write(aggregated_df, unaggregated_df, output_directory, replay_id)
        output_filepath = os.path.join(output_directory, f"{replay_id}_master.mpk.gz")
        agg_data = aggregated_df.to_dicts() if not aggregated_df.is_empty() else []
        unagg_data = unaggregated_df.to_dicts() if not unaggregated_df.is_empty() else []
        master_object: Dict[str, Any] = {"replay_id": replay_id, "map": self._generate_map(aggregated_df.columns, unaggregated_df.columns), "data": {"aggregated": agg_data, "unaggregated": unagg_data}}
        try:
            packed_data = msgpack.packb(master_object, use_bin_type=True)
            with gzip.open(output_filepath, "wb") as f:
                f.write(packed_data)
            logger.info(f"Successfully wrote MessagePack/Gzip output to: {output_filepath}")
        except (TypeError, IOError) as e:
            raise OutputGenerationError(f"Failed to write MessagePack/Gzip output for {replay_id}") from e


class ParquetDirectoryStrategy(OutputStrategy):
    """
    Writes each data stream to a separate Parquet file inside a dedicated directory.
    """
    # ... (This class is unchanged and correct) ...
    def write(self, aggregated_df: pl.DataFrame, unaggregated_df: pl.DataFrame, output_directory: str, replay_id: str) -> None:
        super().write(aggregated_df, unaggregated_df, output_directory, replay_id)
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)
        try:
            if not aggregated_df.is_empty():
                agg_path = os.path.join(replay_output_dir, "aggregated.parquet")
                aggregated_df.write_parquet(agg_path)
                logger.info(f"Successfully wrote aggregated Parquet data to: {agg_path}")
            if not unaggregated_df.is_empty():
                unagg_path = os.path.join(replay_output_dir, "unaggregated.parquet")
                unaggregated_df.write_parquet(unagg_path)
                logger.info(f"Successfully wrote unaggregated Parquet data to: {unagg_path}")
        except Exception as e:
            raise OutputGenerationError(f"Failed to write Parquet output for {replay_id}") from e


class JsonLinesGzipStrategy(OutputStrategy):
    """
    Writes each data stream to a separate, gzipped JSON Lines (.jsonl.gz) file.
    This implementation respects the distinct overloads of `write_ndjson`.
    """
    def _write_single_stream(self, df: pl.DataFrame, output_path: str):
        """Helper to serialize a single DataFrame to a gzipped NDJSON file."""
        if df.is_empty():
            logger.warning(f"DataFrame is empty, skipping write to {output_path}")
            return
            
        # 1. Use the first overload to serialize the DataFrame to an in-memory NDJSON string.
        #    write_ndjson(file=None) -> str
        ndjson_string = df.write_ndjson()

        # 2. Use Python's gzip library to compress and write the string (as bytes) to disk.
        try:
            with gzip.open(output_path, 'wb') as f:
                f.write(ndjson_string.encode('utf-8'))
            logger.info(f"Successfully wrote gzipped JSON Lines output to: {output_path}")
        except IOError as e:
            raise OutputGenerationError(f"Failed to write gzipped file to {output_path}") from e

    def write(self, aggregated_df: pl.DataFrame, unaggregated_df: pl.DataFrame, output_directory: str, replay_id: str) -> None:
        super().write(aggregated_df, unaggregated_df, output_directory, replay_id)
        
        try:
            agg_path = os.path.join(output_directory, f"{replay_id}_aggregated.jsonl.gz")
            self._write_single_stream(aggregated_df, agg_path)

            unagg_path = os.path.join(output_directory, f"{replay_id}_unaggregated.jsonl.gz")
            self._write_single_stream(unaggregated_df, unagg_path)
        except Exception as e:
            # Catch exceptions from the helper and wrap them
            raise OutputGenerationError(f"Failed to write JSON Lines output for {replay_id}") from e