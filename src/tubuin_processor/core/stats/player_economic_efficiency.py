"""
Calculates the economic efficiency of each player.
"""
import polars as pl
from typing import Dict

from .types import Stat

def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Calculates end-of-game damage output per unit of resource spent for each player."""
    team_stats_df = dataframes.get("team_stats")
    if team_stats_df is None or team_stats_df.is_empty():
        return pl.DataFrame(schema={'player_id': pl.Int64, 'damage_per_resource_unit': pl.Float64})

    end_of_game_stats = team_stats_df.group_by("team_id").agg(
        pl.max("metal_used").alias("total_metal_spent"),
        pl.max("energy_used").alias("total_energy_spent"),
        pl.max("damage_dealt").alias("total_damage_output"),
    )

    return (
        end_of_game_stats
        .with_columns(
            (pl.col("total_damage_output") / (pl.col("total_metal_spent") + pl.col("total_energy_spent") + 1e-6)).alias("damage_per_resource_unit")
        )
        .rename({"team_id": "player_id"})
        .select(["player_id", "total_metal_spent", "total_energy_spent", "total_damage_output", "damage_per_resource_unit"])
        .sort("damage_per_resource_unit", descending=True)
    )

# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Calculates end-of-game damage output per unit of resource spent for each player.",
    default_enabled=True
)