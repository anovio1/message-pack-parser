"""
Description
"""
import polars as pl
from typing import Dict

from .types import Stat

def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    return (
       pl.DataFrame()
    )


# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Description.",
    default_enabled=True,
)