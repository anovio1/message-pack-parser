# Developer's Guide to `aggregator.py` (v3.1)

---

## 1. Overview & Architectural Role

The `src/message_pack_parser/core/aggregator.py` module is the analytical orchestration engine of the parser. Its core responsibility is **not** to contain complex statistical logic itself, but to **manage and execute a registry of independent, statistical functions.**

It is executed in **Step 6** of the pipeline and adheres to a strict API contract:

*   **Input Function:** `perform_aggregations(dataframes_by_aspect: Dict[str, pl.DataFrame], stats_to_compute: List[str])`
*   **Output:** `Tuple[Dict[str, pl.DataFrame], pl.DataFrame]`
    *   **Aggregated Stats:** A dictionary where each key is a statistic's name (e.g., `'damage_by_unit_def'`) and the value is its resulting summary DataFrame.
    *   **Unaggregated Stream:** A single, curated DataFrame representing a detailed, time-sorted log of important raw events.

## 2. The Registry Pattern: The Key to Extensibility

The aggregator is built on the **Registry Pattern** to avoid becoming a monolithic module. This design keeps the system extensible and maintainable by adhering to the Open/Closed Principle.

*   **`Stat` Dataclass:** A container holding the metadata for a single statistic: the function to call (`func`), a user-facing `description`, and a `default_enabled` flag.
*   **`STATS_REGISTRY` Dictionary:** A global dictionary that maps a unique string key to its corresponding `Stat` object. This registry is the "menu" of all available statistics.

## 3. Developer Workflow: Adding a New Stat

Adding a new analysis is a clean, two-step process:
1.  **Write a Standalone Helper Function:** Create a private helper function within `aggregator.py`. It must be stateless and have the signature `_my_helper(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame`.
2.  **Register the Function:** Add a new entry to the `STATS_REGISTRY` dictionary, wrapping your function in the `Stat` dataclass.

## 4. Concrete Implementation Examples

This section provides realistic, fully worked-out examples to serve as a template for your own statistics.

### Example A (Aggregated Stat): Aggression Score

**Goal:** Calculate a per-unit "aggression score" based on how far and how quickly units move from their starting positions. This requires joining and enriching data.

```python
# This is an example of a helper function you would add to aggregator.py

def _calculate_aggression_by_unit(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Scores units by how far and fast they move from their start position.
    Higher scores indicate more aggressive, early-game movement.
    """
    pos_df = dataframes.get("unit_positions")
    if pos_df is None or pos_df.is_empty():
        return pl.DataFrame(schema={"unit_id": pl.Int64, "aggression_score": pl.Float64})

    # Assume a constant frame rate for time calculation
    FRAME_RATE = 30.0

    # 1. Find the starting frame and position for each unit using a group-by.
    start_positions = (
        pos_df.sort("frame")
        .group_by("unit_id")
        .agg(
            pl.first("frame").alias("start_frame"),
            pl.first("x").alias("start_x"),
            pl.first("y").alias("start_y"),
        )
    )

    # 2. Join starting positions back to the main positions log. An 'inner' join is safe here.
    aggression_data = pos_df.join(start_positions, on="unit_id", how="inner")

    # 3. Enrich data with new columns for time delta and distance.
    aggression_data = aggression_data.with_columns(
        # Time in seconds since the unit was first seen
        ((pl.col("frame") - pl.col("start_frame")) / FRAME_RATE).alias("time_since_start"),
        # Euclidean distance from start
        (((pl.col("x") - pl.col("start_x")) ** 2 + (pl.col("y") - pl.col("start_y")) ** 2).sqrt()).alias("distance_from_start")
    ).with_columns(
        # Calculate an "aggression impulse" which weights early movement more heavily.
        # Add a small epsilon to avoid division by zero at the start frame.
        (pl.col("distance_from_start") / (pl.col("time_since_start") + 0.1)).alias("aggression_impulse")
    )

    # 4. Sum the "impulse" for each unit to get a final score and sort the results.
    return (
        aggression_data
        .group_by("unit_id")
        .agg(pl.sum("aggression_impulse").alias("aggression_score"))
        .sort("aggression_score", descending=True)
    )

# To register this stat:
# STATS_REGISTRY["aggression_by_unit"] = Stat(func=_calculate_aggression_by_unit, ...)
```

### Example B (Unaggregated Stream): Unified Event Log

**Goal:** Create a single, time-sorted DataFrame that combines two different event types (unit creation and unit commands) into a unified log. This demonstrates selection, renaming, and concatenation.

```python
# This example would modify the helper for the unaggregated stream.

def _get_unified_event_log(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    unit_events_df = dataframes.get("unit_events")
    commands_log_df = dataframes.get("commands_log")
    
    event_streams = []

    if unit_events_df is not None and not unit_events_df.is_empty():
        # 1. Select and format unit creation events into a standard shape.
        creation_events = (
            unit_events_df
            .filter(pl.col("event_type") == "CREATED")
            .select(
                pl.col("frame"),
                pl.col("unit_id"),
                pl.col("unit_team_id").alias("team_id"),
                pl.lit("UnitCreated").alias("event_type"), # Use a common event type column
                pl.col("unit_def_id").cast(pl.Utf8).alias("details") # Common details column
            )
        )
        event_streams.append(creation_events)

    if commands_log_df is not None and not commands_log_df.is_empty():
        # 2. Select and format command events to match the same standard shape.
        command_events = (
            commands_log_df
            .select(
                pl.col("frame"),
                pl.col("unitId").alias("unit_id"),
                pl.col("teamId").alias("team_id"),
                pl.lit("UnitCommand").alias("event_type"),
                pl.col("cmd_name").cast(pl.Utf8).alias("details")
            )
        )
        event_streams.append(command_events)

    # 3. Combine all curated event streams into one sorted DataFrame.
    if not event_streams:
        return pl.DataFrame()
        
    return pl.concat(event_streams, how="diagonal").sort("frame")
```

## 5. Engineering & Performance Best Practices

*   **Function Granularity:** Each `_calculate...` helper must have a single responsibility. If it becomes too complex, break it down further.
*   **Statelessness & Concurrency:** All helper functions **must be stateless and free of side effects**. This is critical for testability and makes the aggregator safe for potential future concurrency improvements.
*   **Filter Early, Join Late:** This is the most important Polars performance rule. Drastically reduce the size of your DataFrames with `.filter()` *before* performing expensive `.join()` operations.
*   **Join Strategies:** Understand your join types:
    *   `inner`: Only keep rows with matches in both tables.
    *   `left`: Keep all rows from the left table, filling with `null` where no match is found on the right. **You must handle these nulls** using `.fill_null()`, `.drop_nulls()`, or filtering.
    *   `asof`: An extremely powerful tool for time-series. Use it to join a record to the *last known value* from another table (e.g., "what was the team's resource count when this unit died?").
*   **Data Types:** Ensure columns used for joins have matching data types. The pipeline provides enums as `pl.Categorical`, which is highly efficient.

## 6. Testing, Validation, and CLI Integration

*   **Testing:** When adding a new stat, **you must add a corresponding unit test** in `tests/unit/test_aggregator.py`. The pattern is to create small, static Polars DataFrames and use `polars.testing.assert_frame_equal` to verify your function's output.
*   **Registry Validation:** The test suite automatically runs `utils/config_validator.py` to ensure that all registered stats are consistent with the rest of the application's configuration.
*   **CLI Integration:** The `STATS_REGISTRY` directly powers the application's CLI. The `description` field from your `Stat` object will automatically appear in the help text for the `parser list-stats` command, and the keys are used to validate input to the `--stat` flag.

## 7. Data Dictionary and Change Management

*   **Data Dictionary:** Before writing any new aggregation, consult the **`docs/data_dictionary.md`**. This is the single, authoritative source for the schemas of all DataFrames available to you.
*   **Contribution Process:** All new statistics must be submitted via a Pull Request that includes the helper function, its registration in `STATS_REGISTRY`, and a corresponding unit test.