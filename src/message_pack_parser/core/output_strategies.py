"""
Module defining different strategies for writing the final output data.
This implements the Strategy design pattern for output generation.
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Type, Any, List
import os
import polars as pl
import msgpack
import gzip
import logging

from message_pack_parser.core.exceptions import OutputGenerationError

logger = logging.getLogger(__name__)

# Enum for CLI choices
class OutputFormat(str, Enum):
    MPK_GZIP = "mpk-gzip"
    PARQUET_DIR = "parquet-dir"
    JSONL_GZIP = "jsonl-gzip"


class OutputStrategy(ABC):
    """Abstract base class for all output writing strategies."""

    @abstractmethod
    def write(
        self,
        aggregated_stats: Dict[str, pl.DataFrame],
        unaggregated_df: pl.DataFrame,
        output_directory: str,
        replay_id: str
    ) -> None:
        """
        Writes the provided DataFrames to a specified location using a specific format.

        Args:
            aggregated_stats: A dictionary mapping stat names to their aggregated DataFrame.
            unaggregated_df: The DataFrame containing detailed, unaggregated data.
            output_directory: The base directory where output should be saved.
            replay_id: The unique identifier for the replay.
        """
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Executing output strategy: {self.__class__.__name__}")

class MessagePackGzipStrategy(OutputStrategy):
    """Writes a single, gzipped MessagePack file where the 'aggregated_stats' key contains a nested dictionary of all computed stats."""
    def _generate_map(self, agg_stats: Dict[str, pl.DataFrame], unagg_cols: List[str]) -> Dict[str, Any]:
        agg_schemas = { stat_name: {"columns": df.columns} for stat_name, df in agg_stats.items() }
        return {
            "version": "3.1",
            "description": "Master data file containing multiple aggregated stat streams.",
            "data_streams": {
                "aggregated_stats": agg_schemas,
                "unaggregated": {"columns": unagg_cols or []}
            }
        }
    def write(self, aggregated_stats: Dict[str, pl.DataFrame], unaggregated_df: pl.DataFrame, output_directory: str, replay_id: str) -> None:
        super().write(aggregated_stats, unaggregated_df, output_directory, replay_id)
        output_filepath = os.path.join(output_directory, f"{replay_id}_master.mpk.gz")
        agg_data_payload = { stat_name: df.to_dicts() for stat_name, df in aggregated_stats.items() }
        unagg_data = unaggregated_df.to_dicts() if not unaggregated_df.is_empty() else []
        master_object: Dict[str, Any] = {
            "replay_id": replay_id,
            "map": self._generate_map(aggregated_stats, unaggregated_df.columns),
            "data": {"aggregated_stats": agg_data_payload, "unaggregated": unagg_data}
        }
        try:
            packed_data = msgpack.packb(master_object, use_bin_type=True)
            assert isinstance(packed_data, bytes)
            with gzip.open(output_filepath, "wb") as f: f.write(packed_data)
            logger.info(f"Successfully wrote MessagePack/Gzip output to: {output_filepath}")
        except (TypeError, IOError) as e:
            raise OutputGenerationError(f"Failed to write MessagePack/Gzip output for {replay_id}") from e

class ParquetDirectoryStrategy(OutputStrategy):
    """
    Writes each computed aggregated stat to its own named .parquet file 
    inside a dedicated directory for the replay.
    """
    def write(self, aggregated_stats: Dict[str, pl.DataFrame], unaggregated_df: pl.DataFrame, output_directory: str, replay_id: str) -> None:
        super().write(aggregated_stats, unaggregated_df, output_directory, replay_id)
        replay_output_dir = os.path.join(output_directory, replay_id)
        os.makedirs(replay_output_dir, exist_ok=True)
        try:
            for stat_name, df in aggregated_stats.items():
                stat_path = os.path.join(replay_output_dir, f"{stat_name}.parquet")
                df.write_parquet(stat_path)
                logger.info(f"Successfully wrote stat '{stat_name}' to: {stat_path}")
            if not unaggregated_df.is_empty():
                unagg_path = os.path.join(replay_output_dir, "unaggregated.parquet")
                unaggregated_df.write_parquet(unagg_path)
                logger.info(f"Successfully wrote unaggregated Parquet data to: {unagg_path}")
        except Exception as e:
            raise OutputGenerationError(f"Failed to write Parquet output for {replay_id}") from e


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
            with gzip.open(output_path, 'wb') as f:
                f.write(ndjson_string.encode('utf-8'))
            logger.info(f"Successfully wrote gzipped JSON Lines output to: {output_path}")
        except IOError as e:
            raise OutputGenerationError(f"Failed to write gzipped file to {output_path}") from e

    def write(self, aggregated_stats: Dict[str, pl.DataFrame], unaggregated_df: pl.DataFrame, output_directory: str, replay_id: str) -> None:
        super().write(aggregated_stats, unaggregated_df, output_directory, replay_id)
        
        try:
            # 1. Loop through the dictionary of computed aggregated stats.
            for stat_name, df in aggregated_stats.items():
                # 2. Create a unique filename for each stat.
                stat_path = os.path.join(output_directory, f"{replay_id}_{stat_name}.jsonl.gz")
                # 3. Pass the actual DataFrame (not the dictionary) to the helper.
                self._write_single_stream(df, stat_path)

            # 4. Handle the unaggregated stream as before.
            if not unaggregated_df.is_empty():
                unagg_path = os.path.join(output_directory, f"{replay_id}_unaggregated.jsonl.gz")
                self._write_single_stream(unaggregated_df, unagg_path)
        except Exception as e:
            # Catch exceptions from the helper and wrap them
            raise OutputGenerationError(f"Failed to write JSON Lines output for {replay_id}") from e
        
# Map for strategy selection
STRATEGY_MAP: Dict[OutputFormat, Type[OutputStrategy]] = {
    OutputFormat.MPK_GZIP: MessagePackGzipStrategy,
    OutputFormat.PARQUET_DIR: ParquetDirectoryStrategy,
    OutputFormat.JSONL_GZIP: JsonLinesGzipStrategy,
}