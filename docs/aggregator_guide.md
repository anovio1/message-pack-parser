# Aggregator Guide
## 1. Overview

The aggregator system is designed as a **pluggable architecture**. You do not need to modify the core aggregation engine (`aggregator.py`) to add new analytical statistics. Instead, you simply add new Python files to the `stats` sub-package, and the system will automatically discover and register them.

This guide will walk you through the process of creating and adding a new statistic to the parser.

## 2. The `Stat` Object

Every registered statistic is defined by a `Stat` object, which lives in `src/message_pack_parser/core/stats/types.py`. It's a simple container with three fields:

-   `func`: The Python function that contains your Polars logic.
-   `description`: A brief, user-facing string explaining what the stat calculates. This is shown in the CLI help text.
-   `default_enabled`: A boolean flag (`True` or `False`). If `True`, this stat will be computed automatically when the user doesn't specify any `--stat` flags.

## 3. How to Create a New Statistic (Tutorial)

Let's create a new statistic called `total_units_lost` as an example.

### Step 1: Create the Stat Module File

Create a new file inside the stats package:
`src/message_pack_parser/core/stats/total_units_lost.py`

### Step 2: Write the Calculation Function

Inside your new file, write a function that performs the analysis. It must follow a specific signature:

-   **Name:** The function name can be anything (convention is `calculate`).
-   **Input:** It must accept one argument: `dataframes: Dict[str, pl.DataFrame]`. This dictionary contains all the clean, analysis-ready aspect DataFrames, keyed by their name.
-   **Output:** It must return a single Polars DataFrame (`pl.DataFrame`).

```python
# In: src/message_pack_parser/core/stats/total_units_lost.py

import polars as pl
from typing import Dict
from .types import Stat # Import the shared Stat dataclass

def calculate(dataframes: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Calculates the total number of units lost by each player."""
    unit_events_df = dataframes.get("unit_events")

    if unit_events_df is None or unit_events_df.is_empty():
        return pl.DataFrame(schema={'player_id': pl.Int64, 'units_lost': pl.UInt32})

    return (
        unit_events_df
        .filter(pl.col("event_type") == "DESTROYED")
        .group_by("unit_team_id")
        .agg(pl.count().alias("units_lost"))
        .rename({"unit_team_id": "player_id"})
        .sort("units_lost", descending=True)
    )
```

### Step 3: Define and Register the Stat

At the bottom of the *same file*, create an instance of the `Stat` object. **It must be named `STAT_DEFINITION`** for the dynamic discovery system to find it.

```python
# In: src/message_pack_parser/core/stats/total_units_lost.py
# (at the bottom of the file)

STAT_DEFINITION = Stat(
    func=calculate,
    description="Counts the total number of units lost by each player.",
    default_enabled=False # We don't want this running by default
)
```

### Step 4: Run the Parser

That's it! No other files need to be modified. The dynamic registry in `stats/__init__.py` will automatically find your new module.

You can now run the parser and use your new stat:

1.  **List all stats to see yours:**
    ```bash
    parser list-stats
    # Output will now include:
    #   - total_units_lost: Counts the total number of units lost by each player.
    ```
2.  **Compute your specific stat:**
    ```bash
    parser run <REPLAY_ID> ... -s total_units_lost
    ```

## 4. Best Practices

-   **One File Per Stat:** Keep each logical statistic in its own file for clarity and maintainability. For complex stats that share helper functions, you can group them in one file, but clearly separate the `STAT_DEFINITION` variables.
-   **Reference the Data Dictionary:** Before writing any code, consult `docs/data_dictionary.md` to understand the available DataFrames and their schemas.
-   **Handle Missing Data:** Always check if a required DataFrame exists and is not empty (e.g., `dataframes.get("some_log")`) and return an empty, correctly-schemed DataFrame if it's missing.
-   **Write Unit Tests:** Add a corresponding test file in `tests/unit/stats/` to validate your logic with sample data.

## Available Data: The Data Dictionary
**Please refer to `docs/data_dictionary.md` for the complete list of all available data and their schemas.**

## Polars Best Practices

*   **Filter Early:** Filter DataFrames to only the rows you need *before* performing expensive joins.
*   **Use Expressions:** Chain operations (`.filter().group_by().agg()`) for efficient, readable code.
*   **Handle Nulls:** Be explicit about how you handle missing data using `.fill_null()`, `.drop_nulls()`, or `.is_not_null()` filters.
*   **Consult the Docs:** The Polars documentation is excellent. Refer to it for advanced expressions and functionality.