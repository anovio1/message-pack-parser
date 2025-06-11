"""
Calculates total metal/energy production and usage per team (single player).
"""
import polars as pl
from typing import Dict

from .types import Stat

def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Calculates total metal/energy production and usage per player."""
    team_stats_df = dataframes.get("team_stats")
    if team_stats_df is None or team_stats_df.is_empty():
        return pl.DataFrame(
            schema={"team_id": pl.Int64, "total_metal_produced": pl.Float64}
        )
    return (
        team_stats_df.group_by("team_id")
        .agg(
            pl.sum("metal_produced").alias("total_metal_produced"),
            pl.sum("metal_used").alias("total_metal_used"),
            pl.sum("energy_produced").alias("total_energy_produced"),
            pl.sum("energy_used").alias("total_energy_used"),
        )
        .sort("team_id")
    )


# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Calculates total metal/energy production and usage per team (single player).",
    default_enabled=True,
)