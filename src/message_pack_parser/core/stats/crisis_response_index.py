"""
[Advanced] Measures player reaction time when core assets are attacked.
"""
import polars as pl
import logging
from typing import Dict

from .types import Stat
from message_pack_parser.config.enums import CommandsEnum

logger = logging.getLogger(__name__)

def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Measures player reaction time and effectiveness when their core assets are attacked.
    This identifies "crisis" events and analyzes the player's first meaningful response.
    """
    damage_log_df = dataframes.get("damage_log")
    commands_log_df = dataframes.get("commands_log")
    unit_events_df = dataframes.get("unit_events")

    # --- Section 1: Setup and Input Validation ---

    # Define constants for the analysis
    LULL_IN_COMBAT_FRAMES = 450  # 15 seconds at 30fps
    RESPONSE_WINDOW_FRAMES = 900  # 30 seconds

    # Define what constitutes a "meaningful" response command
    RELEVANT_RESPONSE_COMMANDS = [
        CommandsEnum.ATTACK,
        CommandsEnum.FIGHT,
        CommandsEnum.REPAIR,
        CommandsEnum.RECLAIM,
        CommandsEnum.GUARD,
        CommandsEnum.MOVE,
    ]

    # Define the final, consistent schema for both empty and populated outputs
    FINAL_SCHEMA = {
        "player_id": pl.Int64,
        "crisis_id": pl.UInt32,
        "victim_unit_id": pl.Int64,
        "crisis_start_frame": pl.Int64,
        "time_to_response_frames": pl.Int64,
        "asset_survived": pl.Boolean,
    }

    if any(
        df is None or df.is_empty()
        for df in [damage_log_df, commands_log_df, unit_events_df]
    ):
        logger.warning(
            "Stat 'crisis_response_index' requires damage_log, commands_log, and unit_events. One or more are missing or empty."
        )
        return pl.DataFrame(schema=FINAL_SCHEMA)

    # --- Section 2: Build LazyFrame Components ---

    # Component A: Identify the start of each crisis event
    crisis_starts_ldf = (
        damage_log_df.lazy()
        .select(["frame", "victim_unit_id", "victim_team_id"])
        .filter(pl.col("victim_unit_id").is_not_null())
        .sort("frame")
        .with_columns(
            (pl.col("frame").diff().over("victim_unit_id") > LULL_IN_COMBAT_FRAMES)
            .fill_null(True)  # The first hit on a unit is always a new crisis
            .alias("is_crisis_start")
        )
        .with_columns(
            pl.col("is_crisis_start")
            .cum_sum()
            .over("victim_unit_id")
            .alias("crisis_instance_id")
        )
        .filter(pl.col("is_crisis_start"))
        .group_by(["victim_unit_id", "victim_team_id", "crisis_instance_id"])
        .agg(pl.min("frame").alias("crisis_start_frame"))
        .rename({"victim_team_id": "player_id"})
    )

    # Component B: Filter for only relevant, meaningful response commands
    relevant_commands_ldf = (
        commands_log_df.lazy()
        .select(["frame", "teamId", "cmd_name"])
        .filter(
            pl.col("cmd_name").is_in([cmd.name for cmd in RELEVANT_RESPONSE_COMMANDS])
        )
        .rename({"frame": "response_frame", "teamId": "player_id"})
        .sort("response_frame")
    )

    # Component C: Identify all destroyed units and their time of destruction
    destroyed_assets_ldf = (
        unit_events_df.lazy()
        .filter(pl.col("event_type") == "DESTROYED")
        .select(["unit_id", "frame"])
        .rename({"frame": "destruction_frame"})
        .unique(subset=["unit_id"], keep="first")  # A unit can only be destroyed once
    )

    # --- Section 3: Construct the Full Lazy Pipeline ---

    final_ldf = (
        crisis_starts_ldf
        # Find the first relevant command issued by the player *after* the crisis began
        .join_asof(
            relevant_commands_ldf,
            left_on="crisis_start_frame",
            right_on="response_frame",
            by="player_id",
            strategy="forward",
        )
        # Check if the asset was ultimately destroyed
        .join(
            destroyed_assets_ldf,
            left_on="victim_unit_id",
            right_on="unit_id",
            how="left",
        )
        # Calculate final metrics
        .with_columns(
            # Calculate time to response, handling cases with no response
            (pl.col("response_frame") - pl.col("crisis_start_frame"))
            .fill_null(-1)
            .alias("time_to_response_frames"),
            # Determine if the asset survived within the response window
            (
                ~(
                    pl.col("destruction_frame").is_not_null()
                    & (
                        pl.col("destruction_frame")
                        <= pl.col("crisis_start_frame") + RESPONSE_WINDOW_FRAMES
                    )
                )
            ).alias("asset_survived"),
        )
        # Add a global, sequential ID for each crisis event
        .sort("crisis_start_frame").with_row_count("crisis_id")
        # Final selection to ensure clean output schema
        .select(list(FINAL_SCHEMA.keys()))
    )

    # --- Section 4: Collect the Result ---

    return final_ldf.collect()



# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="[Advanced] Measures player reaction time when core assets are attacked.",
    default_enabled=True,
)