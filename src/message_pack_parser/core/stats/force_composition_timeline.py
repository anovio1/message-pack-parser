"""
Tracks the number of units of each type produced per player, per minute.
"""
import polars as pl
from typing import Dict

from .types import Stat

def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    FRAME_RATE = 30.0
    df = dataframes.get("unit_events")
    if df is None or df.is_empty():
        return pl.DataFrame()

    # 1) Annotate minute and select the three key cols
    created = (
        df.filter(pl.col("event_type") == "CREATED")
        .with_columns(
            (pl.col("frame") / (FRAME_RATE * 60)).floor().cast(pl.Int32).alias("minute")
        )
        .select(["unit_team_id", "minute", "unit_def_id"])
    )

    if created.is_empty():
        return pl.DataFrame()

    # 2) Count per unit type. This `agg` DataFrame is now our source of truth.
    #    It only contains rows for (player, minute, unit_type) combinations that
    #    actually occurred.
    agg = (
        created.group_by(["unit_team_id", "minute", "unit_def_id"])
        .agg(pl.len().alias("units_produced"))
        .rename({"unit_team_id": "player_id"})
    )

    # --- Steps 3, 4, 5, and 6 are now REMOVED ---
    # We no longer create the full time/player/unit grid.

    # 7) Pack the sparse aggregation directly into a List-of-structs column
    return (
        agg.with_columns(  # <--- We now start directly from our sparse 'agg' DataFrame
            # Create the struct from the existing columns
            pl.struct(["unit_def_id", "units_produced"]).alias("unit_stat")
        )
        .group_by(["player_id", "minute"])
        .agg(pl.col("unit_stat").alias("unit_stats"))
        .sort(["player_id", "minute"])
    )

# The STAT_DEFINITION variable is dynamically loaded by the aggregator.
STAT_DEFINITION = Stat(
    func=calculate,
    description="Tracks the number of units of each type produced per player, per minute.",
    default_enabled=True,
)