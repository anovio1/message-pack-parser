"""
Tracks each player's map control (via bounding box) and unit dispersion per minute
"""

import polars as pl
from typing import Dict

from .types import Stat


def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Tracks each player's map control (via bounding box) and unit dispersion per minute."""
    pos_df = dataframes.get("unit_positions")
    if pos_df is None or pos_df.is_empty():
        return pl.DataFrame(
            schema={
                "player_id": pl.Int64,
                "minute": pl.Int32,
                "bounding_box_area": pl.Float64,
            }
        )

    FRAME_RATE = 30.0
    return (
        pos_df.with_columns(
            (pl.col("frame") / (FRAME_RATE * 60)).floor().cast(pl.Int32).alias("minute")
        )
        .group_by(["team_id", "minute"])
        .agg(
            ((pl.max("x") - pl.min("x")) * (pl.max("y") - pl.min("y"))).alias(
                "bounding_box_area"
            ),
            pl.std("x").alias("dispersion_x"),
            pl.std("y").alias("dispersion_y"),
        )
        .rename({"team_id": "player_id"})
        .sort(["player_id", "minute"])
    )


# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Tracks each player's map control (via bounding box) and unit dispersion per minute",
    default_enabled=True,
)
