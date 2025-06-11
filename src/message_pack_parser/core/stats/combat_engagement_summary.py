"""Main calculation logic for combat engagements, parameterized by strategy."""
import polars as pl
from typing import Dict
from enum import Enum
from functools import partial

from .types import Stat


class EngagementStrategyMode(str, Enum):
    GLOBAL = "global"
    PER_UNIT = "per_unit"
    SPATIOTEMPORAL = "spatiotemporal"


def calculate(
    dataframes: Dict[str, pl.DataFrame],
    mode: EngagementStrategyMode,
    lull_seconds: float,
    spatial_threshold: float,
) -> pl.DataFrame:
    damage_log_df = dataframes.get("damage_log")
    if damage_log_df is None or damage_log_df.is_empty():
        return pl.DataFrame(
            schema={
                "engagement_id": pl.Utf8,
                "start_frame": pl.Int64,
                "total_damage": pl.Float64,
            }
        )

    # ... (Full, robust implementation of the engagement logic as before) ...
    FRAME_RATE = 30.0
    LULL_IN_COMBAT_FRAMES = lull_seconds * FRAME_RATE
    engagements_with_id: pl.DataFrame
    if mode == EngagementStrategyMode.GLOBAL:
        engagements_with_id = (
            damage_log_df.lazy()
            .sort("frame")
            .with_columns(
                pl.when(
                    pl.col("frame").diff().fill_null(LULL_IN_COMBAT_FRAMES + 1)
                    > LULL_IN_COMBAT_FRAMES
                )
                .then(1)
                .otherwise(0)
                .cum_sum()
                .alias("engagement_id")
            )
            .collect()
        )
    elif mode == EngagementStrategyMode.PER_UNIT:
        engagements_with_id = (
            damage_log_df.lazy()
            .filter(pl.col("attacker_unit_id").is_not_null())
            .sort(["attacker_unit_id", "frame"])
            .with_columns(
                pl.col("frame")
                .diff()
                .over("attacker_unit_id")
                .fill_null(LULL_IN_COMBAT_FRAMES + 1)
                .alias("time_gap_frames")
            )
            .with_columns(
                (
                    pl.when(pl.col("time_gap_frames") > LULL_IN_COMBAT_FRAMES)
                    .then(1)
                    .otherwise(0)
                    .cum_sum()
                    .over("attacker_unit_id")
                ).alias("local_engagement_id")
            )
            .with_columns(
                (
                    pl.col("attacker_unit_id").cast(str)
                    + "_"
                    + pl.col("local_engagement_id").cast(str)
                ).alias("engagement_id")
            )
            .collect()
        )
    else:  # SPATIOTEMPORAL
        engagements_with_id = (
            damage_log_df.lazy()
            .filter(pl.col("attacker_unit_id").is_not_null())
            .sort("frame")
            .with_columns(
                pl.col("frame").diff().alias("time_gap_frames"),
                (
                    (pl.col("victim_pos_x") - pl.col("victim_pos_x").shift()) ** 2
                    + (pl.col("victim_pos_y") - pl.col("victim_pos_y").shift()) ** 2
                )
                .sqrt()
                .alias("dist_moved"),
            )
            .with_columns(
                pl.when(
                    (
                        pl.col("time_gap_frames").fill_null(LULL_IN_COMBAT_FRAMES + 1)
                        > LULL_IN_COMBAT_FRAMES
                    )
                    | (
                        pl.col("dist_moved").fill_null(spatial_threshold + 1)
                        > spatial_threshold
                    )
                )
                .then(1)
                .otherwise(0)
                .cum_sum()
                .alias("engagement_id")
            )
            .collect()
        )

    initial_summary = engagements_with_id.group_by("engagement_id").agg(
        pl.min("frame").alias("start_frame"),
        pl.max("frame").alias("end_frame"),
        pl.mean("victim_pos_x").alias("location_x"),
        pl.mean("victim_pos_y").alias("location_y"),
        pl.col("attacker_team_id")
        .filter(pl.col("attacker_team_id").is_not_null())
        .unique()
        .alias("involved_players"),
        pl.sum("damage").alias("total_damage"),
    )
    return initial_summary.with_columns(
        ((pl.col("end_frame") - pl.col("start_frame")) / FRAME_RATE).alias(
            "duration_seconds"
        )
    ).sort("start_frame")


# --- Definitions for the registry ---
# We use functools.partial to create specialized versions of our function.
STAT_DEFINITION_DEFAULT = Stat(
    func=partial(
        calculate,
        mode=EngagementStrategyMode.SPATIOTEMPORAL,
        lull_seconds=10.0,
        spatial_threshold=500.0,
    ),
    description="Clusters damage events to identify discrete combat engagements based on time and space.",
    default_enabled=True,
)

STAT_DEFINITION_GLOBAL = Stat(
    func=partial(
        calculate,
        mode=EngagementStrategyMode.GLOBAL,
        lull_seconds=10.0,
        spatial_threshold=0,
    ),
    description="Identifies engagements based on global time gaps only (map-wide 'wartime').",
)

STAT_DEFINITION_PER_UNIT = Stat(
    func=partial(
        calculate,
        mode=EngagementStrategyMode.PER_UNIT,
        lull_seconds=15.0,
        spatial_threshold=0,
    ),
    description="Identifies an individual unit's separate combat encounters.",
)
