"""
Calculates player APM and the percentage of actions dedicated to combat vs. economy per minute.
"""
import polars as pl
from typing import Dict

from .types import Stat

def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    commands_log_df = dataframes.get("commands_log")
    if commands_log_df is None or commands_log_df.is_empty():
        return pl.DataFrame(
            schema={
                "player_id": pl.Int64,
                "minute": pl.Int32,
                "actions_per_minute": pl.UInt32,
            }
        )

    FRAME_RATE = 30.0
    combat_commands = ["ATTACK", "FIGHT", "MANUAL_FIRE"]
    econ_commands = ["BUILD", "RECLAIM", "REPAIR"]

    commands_with_focus = commands_log_df.with_columns(
        (pl.col("frame") / (FRAME_RATE * 60)).floor().cast(pl.Int32).alias("minute"),
        pl.when(pl.col("cmd_name").is_in(combat_commands))
        .then(pl.lit("combat"))
        .when(pl.col("cmd_name").is_in(econ_commands))
        .then(pl.lit("economy"))
        .otherwise(pl.lit("other"))
        .alias("command_category"),
    )

    return (
        commands_with_focus.group_by(["teamId", "minute"])
        .agg(
            pl.count().alias("actions_per_minute"),
            (
                pl.col("command_category")
                .filter(pl.col("command_category") == "combat")
                .count()
                / pl.count()
                * 100
            ).alias("combat_focus_pct"),
            (
                pl.col("command_category")
                .filter(pl.col("command_category") == "economy")
                .count()
                / pl.count()
                * 100
            ).alias("economy_focus_pct"),
        )
        .rename({"teamId": "player_id"})
        .sort(["player_id", "minute"])
    )


# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Calculates player APM and their focus on combat vs. economy per minute.",
    default_enabled=True,
)