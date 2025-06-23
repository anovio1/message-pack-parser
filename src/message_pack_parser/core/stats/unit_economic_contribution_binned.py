import warnings
import polars as pl
from typing import Dict
from .types import Stat

# ---------------------------------------------------------------------------
# Configuration Flags
# ---------------------------------------------------------------------------
INCLUDE_DEBUG_COLS = False   # ← Set True to include time_bin_end_frame
# ---------------------------------------------------------------------------

BINNING_INTERVAL_FRAMES = 300   # 10-second bins at 30 fps
FRAME_RATE              = 30.0  # FPS → seconds


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _apportion_to_bins(intervals_df: pl.DataFrame) -> pl.LazyFrame:
    """
    Slice continuous unit-economy intervals into X-second bins and return:

      team_id  | unit_def_id | bin_start
      ----------------------------------
      total_unit_seconds_in_bin
      units_alive_in_bin
      [ resource totals … ]

    • total_unit_seconds_in_bin = Σ seconds each unit overlapped the bin
    • units_alive_in_bin        = distinct unit count in that bin
    """
    metal_ok  = "metal_prod_s"  in intervals_df.columns
    energy_ok = "energy_prod_s" in intervals_df.columns

    # ── Early exit on empty input ──────────────────────────────────────────
    if intervals_df.is_empty():
        schema = {
            "team_id":                         pl.Int64,
            "unit_def_id":                     pl.Int64,
            "bin_start":                       pl.Int64,
            "total_unit_seconds_in_bin":       pl.Float64,
            "units_alive_in_bin":              pl.Int64,
            "total_units_produced_in_bin":     pl.Int64,
        }
        if metal_ok:
            schema |= {
                "total_metal_produced_in_bin": pl.Float64,
                "total_metal_consumed_in_bin": pl.Float64,
            }
        if energy_ok:
            schema |= {
                "total_energy_produced_in_bin": pl.Float64,
                "total_energy_consumed_in_bin": pl.Float64,
            }
        return pl.DataFrame(schema=schema).lazy()

    # ── Generate only needed bins ──────────────────────────────────────────
    min_bin, max_bin = intervals_df.select(
        (pl.min("start_f") // BINNING_INTERVAL_FRAMES * BINNING_INTERVAL_FRAMES)
        .alias("min_bin"),
        (pl.max("end_f")   // BINNING_INTERVAL_FRAMES * BINNING_INTERVAL_FRAMES)
        .alias("max_bin"),
    ).row(0)

    bins = (
        pl.int_range(min_bin, max_bin + BINNING_INTERVAL_FRAMES,
                     step=BINNING_INTERVAL_FRAMES, eager=True)
          .alias("bin_start")
          .to_frame()
          .with_columns(
              (pl.col("bin_start") + BINNING_INTERVAL_FRAMES).alias("bin_end")
          )
    )

    # ── Cross-join + overlap filter ───────────────────────────────────────
    exploded = (
        intervals_df.lazy()
        .join(bins.lazy(), how="cross")
        .filter(
            pl.max_horizontal("start_f", "bin_start")
            < pl.min_horizontal("end_f", "bin_end")
        )
        .with_columns([
            pl.max_horizontal("start_f", "bin_start").alias("ov_start_f"),
            pl.min_horizontal("end_f",   "bin_end").alias("ov_end_f"),
        ])
        .with_columns(
            ((pl.col("ov_end_f") - pl.col("ov_start_f")) / FRAME_RATE)
            .alias("ov_seconds")
        )
    )

    # ── Apportion resources over overlap seconds ──────────────────────────
    if metal_ok:
        exploded = exploded.with_columns(
            (pl.col("metal_prod_s") * pl.col("ov_seconds")).alias("metal_prod_in_bin"),
            (pl.col("metal_cons_s") * pl.col("ov_seconds")).alias("metal_cons_in_bin"),
        )
    if energy_ok:
        exploded = exploded.with_columns(
            (pl.col("energy_prod_s") * pl.col("ov_seconds")).alias("energy_prod_in_bin"),
            (pl.col("energy_cons_s") * pl.col("ov_seconds")).alias("energy_cons_in_bin"),
        )

    # ── Aggregate ─────────────────────────────────────────────────────────
    available_cols = exploded.collect_schema().names()

    agg_exprs: list[pl.Expr] = [
        pl.sum("ov_seconds").alias("total_unit_seconds_in_bin"),
        pl.n_unique("unit_id").alias("units_alive_in_bin"),
    ]
    if "metal_prod_in_bin" in available_cols:
        agg_exprs += [
            pl.sum("metal_prod_in_bin").alias("total_metal_produced_in_bin"),
            pl.sum("metal_cons_in_bin").alias("total_metal_consumed_in_bin"),
        ]
    if "energy_prod_in_bin" in available_cols:
        agg_exprs += [
            pl.sum("energy_prod_in_bin").alias("total_energy_produced_in_bin"),
            pl.sum("energy_cons_in_bin").alias("total_energy_consumed_in_bin"),
        ]

    return (
        exploded
        .group_by("team_id", "unit_def_id", "bin_start")
        .agg(agg_exprs)
    )


# ---------------------------------------------------------------------------
# Main calculation (unchanged except for new columns)
# ---------------------------------------------------------------------------
def _calculate_binned_economic_activity(
    dataframes: Dict[str, pl.DataFrame]
) -> pl.DataFrame:

    unit_economy_df = dataframes.get("unit_economy")
    unit_events_df  = dataframes.get("unit_events")

    if unit_economy_df is None or unit_economy_df.is_empty():
        return pl.DataFrame()

    # --- Build continuous intervals
    metal_ok  = {"metal_make",  "metal_use"}  <= set(unit_economy_df.columns)
    energy_ok = {"energy_make", "energy_use"} <= set(unit_economy_df.columns)

    rate_cols: list[pl.Expr] = []
    if metal_ok:
        rate_cols += [
            pl.col("metal_make").fill_null(0.0).alias("metal_prod_s"),
            pl.col("metal_use").fill_null(0.0).alias("metal_cons_s"),
        ]
    if energy_ok:
        rate_cols += [
            pl.col("energy_make").fill_null(0.0).alias("energy_prod_s"),
            pl.col("energy_use").fill_null(0.0).alias("energy_cons_s"),
        ]

    intervals = (
        unit_economy_df
        .sort("unit_id", "frame")
        .with_columns(
            pl.col("frame").shift(-1).over("unit_id").alias("end_f")
        )
        .filter(pl.col("end_f").is_not_null())
        .rename({"frame": "start_f"})
        .with_columns(rate_cols)
    )

    # --- Units produced per bin (may be empty)
    production_counts = pl.DataFrame(
        schema={
            "team_id":                     pl.Int64,
            "unit_def_id":                 pl.Int64,
            "bin_start":                   pl.Int64,
            "total_units_produced_in_bin": pl.Int64,
        }
    )

    if unit_events_df is not None and not unit_events_df.is_empty():
        production_counts = (
            unit_events_df
            .filter(pl.col("event_type") == "CREATED")
            .with_columns(
                (pl.col("frame") // BINNING_INTERVAL_FRAMES * BINNING_INTERVAL_FRAMES)
                .alias("bin_start")
            )
            .group_by("unit_team_id", "unit_def_id", "bin_start")
            .agg(pl.n_unique("unit_id").alias("total_units_produced_in_bin"))
            .rename({"unit_team_id": "team_id"})
        )

    # --- Bin-level resource contributions
    binned_lazy = _apportion_to_bins(intervals)

    # --- Outer join (only if production data exists)
    if not production_counts.is_empty():
        binned_lazy = binned_lazy.join(
            production_counts.lazy(),
            on=["team_id", "unit_def_id", "bin_start"],
            how="outer",
        )

    # --- Fill nulls & guard keys
    metric_defaults = {
        "total_metal_produced_in_bin": 0.0,
        "total_metal_consumed_in_bin": 0.0,
        "total_energy_produced_in_bin": 0.0,
        "total_energy_consumed_in_bin": 0.0,
        "total_unit_seconds_in_bin":   0.0,
        "units_alive_in_bin":          0,
        "total_units_produced_in_bin": 0,
    }
    available_cols = binned_lazy.collect_schema().names()

    binned_lazy = binned_lazy.with_columns([
        pl.col(c).fill_null(v) for c, v in metric_defaults.items()
        if c in available_cols
    ]).filter(
        pl.all_horizontal(
            pl.col("team_id").is_not_null(),
            pl.col("unit_def_id").is_not_null(),
            pl.col("bin_start").is_not_null(),
        )
    )

    # --- Final polish: optionally emit debug column ─────────────────────
    if INCLUDE_DEBUG_COLS:
        warnings.warn(
            "⚠️  Including debug column `time_bin_end_frame` in output",
            stacklevel=2
        )
        binned_lazy = binned_lazy.with_columns(
            (pl.col("bin_start") + BINNING_INTERVAL_FRAMES)
            .alias("time_bin_end_frame")
        )

    available_cols = binned_lazy.collect_schema().names()

    # --- Build the select list ────────────────────────────────────────────
    select_cols = [
        "team_id", "unit_def_id",
        pl.col("bin_start").alias("time_bin_start_frame"),
        "units_alive_in_bin",
        "total_unit_seconds_in_bin",
        "total_units_produced_in_bin",
    ]

    if INCLUDE_DEBUG_COLS:
        select_cols.append("time_bin_end_frame")

    for col in [
        "total_metal_produced_in_bin", "total_metal_consumed_in_bin",
        "total_energy_produced_in_bin", "total_energy_consumed_in_bin",
    ]:
        if col in available_cols:
            select_cols.append(col)

    return (
        binned_lazy
        .select(select_cols)
        .sort(["time_bin_start_frame", "team_id", "unit_def_id"])
        .collect()
    )


# ---------------------------------------------------------------------------
# Stat registration
# ---------------------------------------------------------------------------
STAT_DEFINITION = Stat(
    func=_calculate_binned_economic_activity,
    description=(
        "Per-bin resource production/consumption, concurrent unit count, and "
        "unit-seconds of lifetime (5-second bins)."
    ),
    default_enabled=True,
)
