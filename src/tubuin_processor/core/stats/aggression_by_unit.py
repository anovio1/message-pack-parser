"""
Scores units by distance-over-time from their start position.
"""

import polars as pl
from typing import Dict

from .types import Stat


def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Scores units by how far and fast they move from their start position.
    Higher scores indicate more aggressive, early-game movement.
    """
    pos_df = dataframes.get("unit_positions")
    if pos_df is None or pos_df.is_empty():
        return pl.DataFrame(
            schema={"unit_id": pl.Int64, "aggression_score": pl.Float64}
        )

    # Assume a constant frame rate for time calculation
    FRAME_RATE = 30.0

    # 1. Find the starting frame and position for each unit
    start_positions = (
        pos_df.sort("frame")
        .group_by("unit_id")
        .agg(
            pl.first("frame").alias("start_frame"),
            pl.first("x").alias("start_x"),
            pl.first("y").alias("start_y"),
        )
    )

    # 2. Join starting positions back to the main positions log
    aggression_data = pos_df.join(start_positions, on="unit_id")

    # 3. Calculate distance from start and "aggression impulse" (distance/time)
    # The aggression impulse weights early movement more heavily.
    aggression_data = aggression_data.with_columns(
        # Time in seconds since the unit was first seen
        ((pl.col("frame") - pl.col("start_frame")) / FRAME_RATE).alias(
            "time_since_start"
        ),
        # Euclidean distance from start
        (
            (
                (pl.col("x") - pl.col("start_x")) ** 2
                + (pl.col("y") - pl.col("start_y")) ** 2
            ).sqrt()
        ).alias("distance_from_start"),
    ).with_columns(
        # Avoid division by zero for the very first frame.
        # This formula gives a score for movement over time.
        (pl.col("distance_from_start") / (pl.col("time_since_start") + 0.1)).alias(
            "aggression_impulse"
        )
    )

    # 4. Sum the "impulse" for each unit to get a final score
    return (
        aggression_data.group_by("unit_id")
        .agg(pl.sum("aggression_impulse").alias("aggression_score"))
        .sort("aggression_score", descending=True)
    )


# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Scores units by distance-over-time from their start position.",
    default_enabled=True,
)
