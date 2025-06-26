from typing import Dict
import polars as pl
import logging

from tubuin_processor.core.exceptions import AggregationError
from tubuin_processor.core.stats.types import Stat

logger = logging.getLogger(__name__)

# --- Configuration ---
FRAME_RATE = 30  # game ticks per second
INTERVAL_SECONDS = 15  # sampling interval


def force_int_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    If a column is a struct, unnest it. Always ensures the final column is Int64.
    """
    if col not in df.columns:
        return df

    if isinstance(df[col].dtype, pl.Struct):
        # Unnest only the target column. Passing a list is safer.
        df = df.unnest([col])

    # Always cast after any possible unnesting.
    return df.with_columns(pl.col(col).cast(pl.Int64))


# ──────────────────────────────────────────────────────────────────────────────
# Main stat function
# ──────────────────────────────────────────────────────────────────────────────
def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    logger.info("Calculating stat: Army Value Timeline")

    # 1. Inputs
    unit_events_df = dataframes["unit_events"]
    unit_defs_df = dataframes["unit_defs"]
    defs_map_df = dataframes["defs_map"]


    # 2. Clean frame
    unit_events_df = force_int_column(unit_events_df, "frame")

    # 3. Clean maps
    clean_defs_map = defs_map_df.select(["unit_def_id", "unit_name"])
    clean_unit_defs = unit_defs_df.select(["unit_name", "metalcost"])

    # 4. Max frame
    frame_col = unit_events_df["frame"].cast(pl.Int64)
    max_frame_value = frame_col.max()
    if max_frame_value is None:
        max_frame = 0
    elif isinstance(max_frame_value, (int, float)):
        max_frame = int(max_frame_value)
    else:
        max_frame = 0

    # 5. Lifespans
    finished = (
        unit_events_df.filter(pl.col("event_type") == "FINISHED")
        .select(["frame", "unit_id", "unit_def_id", "unit_team_id"])
        .rename({"frame": "creation_frame"})
    )
    destroyed = (
        unit_events_df.filter(pl.col("event_type") == "DESTROYED")
        .select(["frame", "unit_id"])
        .rename({"frame": "death_frame"})
    )
    unit_lifespans = finished.join(destroyed, on="unit_id", how="left").with_columns(
        pl.col("death_frame").fill_null(max_frame + 1)
    )

    unit_lifespans = force_int_column(unit_lifespans, "death_frame")

    # 6. Attach costs
    jm1 = unit_lifespans.join(clean_defs_map, on="unit_def_id", how="left")

    jm2 = jm1.join(clean_unit_defs, on="unit_name", how="left")

    unit_lifespans_with_cost = jm2.filter(
        pl.col("unit_name").is_not_null()
    ).with_columns(pl.col("metalcost").fill_null(0.0).cast(pl.Float64))

    unit_lifespans_with_cost = force_int_column(unit_lifespans_with_cost, "death_frame")

    # early exit
    if unit_lifespans_with_cost.is_empty():
        return pl.DataFrame(
            schema={"team_id": pl.Int64, "frame": pl.Int64, "army_value": pl.Float64}
        )

    # 7. Creation & destruction deltas
    creations_delta = unit_lifespans_with_cost.select(
        pl.col("creation_frame").alias("frame"),
        pl.col("unit_team_id").alias("team_id"),
        pl.col("metalcost").alias("value_change"),
    )

    tmp = unit_lifespans_with_cost.filter(pl.col("death_frame") <= max_frame)

    destructions_delta = tmp.select(
        pl.col("death_frame").alias("frame"),
        pl.col("unit_team_id").alias("team_id"),
        (-pl.col("metalcost")).alias("value_change"),
    )

    all_value_changes = pl.concat([creations_delta, destructions_delta])

    army_value_log = (
        all_value_changes.group_by(["team_id", "frame"])
        .agg(pl.sum("value_change").alias("net_change_at_frame"))
        .sort(["team_id", "frame"])
        .with_columns(
            pl.col("net_change_at_frame").cum_sum().over("team_id").alias("army_value")
        )
        .select(["team_id", "frame", "army_value"])
    )

    # 8. Build timeline
    interval_frames = INTERVAL_SECONDS * FRAME_RATE
    base_tl = pl.DataFrame(
        {
            "frame": pl.int_range(
                0, max_frame, interval_frames, eager=True, dtype=pl.Int64
            )
        }
    )
    last_tl = pl.DataFrame({"frame": [max_frame]})
    timeline_df = pl.concat([base_tl, last_tl]).unique(subset=["frame"]).sort("frame")

    # 9. Cross-join with teams
    all_teams = army_value_log["team_id"].unique(maintain_order=True)
    expanded_tl = timeline_df.join(
        pl.DataFrame({"team_id": all_teams}), how="cross"
    ).sort("frame")

    # 0) as-of join – still delivers Float64
    joined = expanded_tl.join_asof(
        army_value_log,
        on="frame",
        by="team_id",
    )

    # Build the “cleaned” army_value once as an expression
    army_expr = (
        pl.col("army_value")
        .forward_fill().over("team_id")   # carry last value
        .fill_null(0.0)                   # leading gaps → 0
    )

    # 1-shot select: frame + team_id untouched, army_value cleaned & clamped
    final = joined.select([
        "frame",           # stays Int64
        "team_id",         # stays Int64
        pl.when(army_expr < 0.0)
        .then(0.0)
        .otherwise(army_expr)
        .alias("army_value")              # stays Float64
    ])

    return final


# ──────────────────────────────────────────────────────────────────────────────
# Stat registration object
# ──────────────────────────────────────────────────────────────────────────────
STAT_DEFINITION = Stat(
    func=calculate,
    description="Calculates the total army value for each team at fixed time intervals.",
    default_enabled=True,
)
