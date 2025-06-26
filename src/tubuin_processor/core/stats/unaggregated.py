# src\tubuin_processor\core\stats\unaggregated.py
"""
Defines functions that produce detailed, unaggregated data streams.
"""
import polars as pl
from typing import Dict

def get_detailed_command_log(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Returns the cleaned command log as a detailed event stream."""
    commands_log_df = dataframes.get("commands_log")
    return (
        pl.DataFrame()
        if commands_log_df is None or commands_log_df.is_empty()
        else commands_log_df.sort("frame")
    )