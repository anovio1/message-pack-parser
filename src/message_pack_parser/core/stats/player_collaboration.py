"""
Summarizes each player's resource sharing, identifying net donors and receivers.
"""

import polars as pl
from typing import Dict

from .types import Stat


def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    team_stats_df = dataframes.get("team_stats")
    if team_stats_df is None or team_stats_df.is_empty():
        return pl.DataFrame(
            schema={"player_id": pl.Int64, "net_metal_contribution": pl.Float64}
        )

    final_sharing_stats = team_stats_df.group_by("team_id").agg(
        pl.max("metal_sent").alias("total_metal_sent"),
        pl.max("metal_received").alias("total_metal_received"),
        pl.max("energy_sent").alias("total_energy_sent"),
        pl.max("energy_received").alias("total_energy_received"),
    )

    return (
        final_sharing_stats.with_columns(
            (pl.col("total_metal_sent") - pl.col("total_metal_received")).alias(
                "net_metal_contribution"
            ),
            (pl.col("total_energy_sent") - pl.col("total_energy_received")).alias(
                "net_energy_contribution"
            ),
        )
        .rename({"team_id": "player_id"})
        .sort("net_metal_contribution", descending=True)
    )


# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Summarizes each player's resource sharing, identifying net donors and receivers.",
    default_enabled=True,
)
