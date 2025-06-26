"""
Calculates total damage dealt per unit definition ID.
"""

import polars as pl
from typing import Dict

from .types import Stat


def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Calculates total damage dealt per unit definition ID."""
    damage_log_df = dataframes.get("damage_log")
    if damage_log_df is None or damage_log_df.is_empty():
        return pl.DataFrame(
            schema={"unit_def_id": pl.Int64, "total_damage_dealt": pl.Float64}
        )
    return (
        damage_log_df.filter(pl.col("attacker_def_id").is_not_null())
        .group_by("attacker_def_id")
        .agg(pl.sum("damage").alias("total_damage_dealt"))
        .rename({"attacker_def_id": "unit_def_id"})
        .sort("total_damage_dealt", descending=True)
    )


# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Calculates total damage dealt per unit definition ID.",
    default_enabled=True,
)
