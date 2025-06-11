Of course. Here is a comprehensive documentation page for `aggregator.py`. This document is designed to be thorough enough for a new developer to understand the module's role, its current state, and exactly how to extend it with production-level logic.

It could be saved as `docs/aggregator_guide.md` or as the primary docstring within the `aggregator.py` file itself.

---

# `aggregator.py` - Design & Extension Guide

**Version:** 1.0
**Author:** [Your Name/Team]

## 1. Overview

The `src/core/aggregator.py` module is the analytical heart of the Message Pack Parser pipeline. Its primary responsibility is to take the collection of clean, structured Polars DataFrames (one for each aspect) and transform them into meaningful, high-level insights.

This is where the raw, time-series data is converted into answers for specific questions about the game, such as:
*   "Which team had the highest resource income rate?"
*   "What were the key battles, and who participated?"
*   "Which player was the most aggressive, based on unit movements and damage dealt?"

This module is intentionally designed as a pluggable component. The initial implementation provides a safe, non-functional placeholder and an optional demonstration. **The core task for the user is to replace this placeholder with domain-specific aggregation logic.**

## 2. Role in the Pipeline

`aggregator.py` is executed in **Step 6** of the parsing process.

*   **Input:** It receives a dictionary where keys are aspect names (e.g., `'team_stats'`, `'unit_positions'`) and values are the corresponding, fully-processed Polars DataFrames from **Step 5 (`dataframe_creator.py`)**. At this point, the data is:
    *   Clean and validated.
    *   Dequantized (integers converted to floats where specified).
    *   Type-safe (e.g., Enum values are represented as Polars' `Categorical` type).
*   **Output:** It produces a `Tuple` containing exactly two Polars DataFrames:
    1.  `aggregated_df`: A DataFrame containing summary data. This is typically smaller, with one row per entity being summarized (e.g., per team, per player).
    2.  `unaggregated_df`: A DataFrame containing a curated stream of detailed, time-series events. This is not just a dump of all input data; it's often a combination of important events (like unit commands, damage instances, construction events) sorted chronologically.

## 3. Core Function: `perform_aggregations`

```python
def perform_aggregations(
    dataframes_by_aspect: Dict[str, pl.DataFrame],
    run_demo_logic: bool = False
) -> Tuple[pl.DataFrame, pl.DataFrame]:
```

*   `dataframes_by_aspect`: The dictionary of input Polars DataFrames.
*   `run_demo_logic`: A boolean flag passed from the CLI (`--run-demo-aggregation`). If `True`, the illustrative logic runs. If `False` (the default), the function will raise a `NotImplementedError` until you add your production logic.

### 3.1. Current (Illustrative) Implementation

The file contains a helper function `_perform_illustrative_aggregation` which demonstrates basic functionality:

1.  **Aggregated Stream:** It takes the `team_stats` DataFrame, groups it by `team_id`, and calculates the sum of `metal_produced` and `damage_dealt` for each team.
2.  **Unaggregated Stream:** It simply takes the `unit_positions` DataFrame and sorts it by frame, presenting it as a time-series event log.

This demo serves as a "hello world" to show how to access and manipulate the input DataFrames.

## 4. How to Extend for Production Logic

This is the primary task for a developer using this pipeline. The process involves defining the analytical questions you want to answer and translating them into Polars code.

### Step 1: Define Your Analytical Goals
Before writing any code, clearly define what you want to compute. For example:

*   **Goal A (Aggregation):** Calculate each player's "Aggression Score" per minute. An aggression score could be a combination of distance traveled towards the enemy and damage dealt.
*   **Goal B (Unaggregated Stream):** Create a unified "Event Log" that combines unit creation events and command events into a single, sorted timeline.

### Step 2: Implement the Logic
You will replace the `NotImplementedError` in `perform_aggregations` with your code.

#### Example Implementation for Goal A (Aggression Score)

This is a complex example that requires joining multiple DataFrames.

```python
# --- Inside perform_aggregations function ---

# 1. Access required DataFrames
damage_log_df = dataframes_by_aspect.get("damage_log")
unit_positions_df = dataframes_by_aspect.get("unit_positions")
team_stats_df = dataframes_by_aspect.get("team_stats") # To link unit teams to players

if damage_log_df is None or unit_positions_df is None or team_stats_df is None:
    raise AggregationError("Missing required DataFrames (damage_log, unit_positions, team_stats) for aggression analysis.")

# 2. Pre-process and enrich data
# For time-series analysis, converting frame numbers to seconds or minutes is useful.
FRAME_RATE = 30
damage_log_df = damage_log_df.with_columns(
    (pl.col("frame") / (FRAME_RATE * 60)).floor().cast(pl.Int32).alias("minute")
)
unit_positions_df = unit_positions_df.with_columns(
    (pl.col("frame") / (FRAME_RATE * 60)).floor().cast(pl.Int32).alias("minute")
)

# 3. Calculate damage per unit per minute
damage_per_unit_minute = damage_log_df.group_by(["attacker_unit_id", "minute"]).agg(
    pl.sum("damage").alias("damage_dealt")
).rename({"attacker_unit_id": "unit_id"})

# 4. Join damage data with position data
# This is a many-to-many join on (unit_id, minute), combining all positions
# of a unit in a minute with all the damage it dealt in that same minute.
aggression_df = unit_positions_df.join(
    damage_per_unit_minute,
    on=["unit_id", "minute"],
    how="left" # Use 'left' to keep units that moved but didn't deal damage
).fill_null(0) # Fill damage_dealt with 0 for non-damaging units

# 5. Calculate final Aggression Score per team per minute
# This is a placeholder formula.
final_aggression_score_df = aggression_df.group_by(["team_id", "minute"]).agg(
    (pl.sum("damage_dealt") * 0.75 + pl.mean("vx").abs() * 0.25).alias("aggression_score")
).sort(["team_id", "minute"])

# 6. Assign this as your primary aggregated output
aggregated_df = final_aggression_score_df
```

#### Example Implementation for Goal B (Unified Event Log)

This example focuses on curating and combining streams.

```python
# --- Inside perform_aggregations function, for the unaggregated stream ---

unit_events_df = dataframes_by_aspect.get("unit_events")
commands_log_df = dataframes_by_aspect.get("commands_log")

event_streams = []

if unit_events_df is not None:
    # Select, rename, and format unit creation events
    creation_events = (
        unit_events_df
        .filter(pl.col("event_type") == "CREATED")
        .select(
            pl.col("frame"),
            pl.col("unit_id"),
            pl.col("unit_team_id").alias("team_id"),
            pl.lit("Unit Created").alias("event_description"),
            pl.col("unitDefID").cast(pl.Utf8).alias("details")
        )
    )
    event_streams.append(creation_events)

if commands_log_df is not None:
    # Select, rename, and format command events
    command_events = (
        commands_log_df
        .select(
            pl.col("frame"),
            pl.col("unitId").alias("unit_id"),
            pl.col("teamId").alias("team_id"),
            pl.lit("Unit Command Issued").alias("event_description"),
            pl.col("cmd_name").cast(pl.Utf8).alias("details")
        )
    )
    event_streams.append(command_events)

# Combine all curated event streams into one DataFrame
if event_streams:
    unaggregated_df = pl.concat(event_streams, how="diagonal").sort("frame")
```

### Step 3: Integrate into `perform_aggregations`

Finally, you would replace the `NotImplementedError` block with your new logic:

```python
def perform_aggregations(
    dataframes_by_aspect: Dict[str, pl.DataFrame],
    run_demo_logic: bool = False
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger.info("Starting Step 6: Data Aggregation and Stream Generation")
    if run_demo_logic:
        return _perform_illustrative_aggregation(dataframes_by_aspect)

    try:
        # --- Your production logic from Step 2 goes here ---
        # (e.g., the code for Goal A and Goal B)
        aggregated_df = ...
        unaggregated_df = ...
        # --- End of your production logic ---

        logger.info(f"Aggregation complete. Aggregated shape: {aggregated_df.shape}, Unaggregated shape: {unaggregated_df.shape}")
        return aggregated_df, unaggregated_df
        
    except Exception as e:
        logger.error(f"An error occurred during the production aggregation process: {e}", exc_info=True)
        raise AggregationError("Aggregation failed") from e
```

## 5. Best Practices & Design Patterns

*   **Start with Questions:** Your aggregation logic should be driven by the specific insights you need.
*   **Use Helper Functions:** For complex aggregations, break down the logic into smaller, testable helper functions (e.g., `_calculate_aggression_score`, `_create_event_log`).
*   **Chain Polars Expressions:** Use Polars' method chaining for readability. Assign intermediate DataFrames to descriptive variable names to document the process.
*   **Filter Early, Join Late:** To improve performance, filter DataFrames to only include necessary rows *before* performing expensive join operations.
*   **Understand Your Joins:** The most valuable insights often come from joining DataFrames. Choose your join strategy carefully:
    *   `inner`: For records that exist in both tables.
    *   `left`: To keep all records from the left table, even if there's no match.
    *   `asof`: For joining time-series data on the most recent matching key (extremely useful for game state snapshots).
*   **Handle Nulls Explicitly:** After a join, `null` values will appear. Use `fill_null()` or `.is_not_null()` filters to handle them intentionally.

## 6. Performance Considerations

*   **Memory:** Joins on large DataFrames can significantly increase memory usage. Use the techniques above (filtering early) to mitigate this. Polars is generally memory-efficient, but be mindful of creating many large intermediate tables.
*   **Join Keys:** Ensure the columns you are joining on (`on=[...]`) are of the same data type. Polars is fast, but type casting during a join can slow it down.
*   **Categorical Type:** Polars' `Categorical` type (used for enums) is very efficient for joins and group-by operations on string-like data. Use it wherever possible for columns with a limited set of unique values.

---