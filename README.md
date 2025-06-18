# Message Pack Processor

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Pytest](https://img.shields.io/badge/tested%20with-pytest-009ee5.svg)](https://pytest.org)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A parallelized processor for ingesting, processing, and aggregating data from `.mpk` files.

This tool is designed to be robust, efficient, and extensible, using a modern Python stack including Pydantic, Polars, and Typer.

## Core Features

- **Schema-Driven Configuration:** A single source of truth (`aspects_raw.py`) defines both the raw data structure and all transformation rules (dequantization, enum mapping), eliminating configuration drift.
- **Pluggable Analytics:** New summary statistics can be added as simple drop-in Python files without modifying the core processing engine.
- **Configurable Aggregation:** Users can select which summary statistics to compute at runtime.
- **High-Performance Processing:**
  - **Parallel Execution:** Leverages `ProcessPoolExecutor` to process multiple large data files concurrently, maximizing CPU usage.
  - **Streaming Decoder:** Uses a streaming `msgpack.Unpacker` to handle arbitrarily large aspect files with minimal memory footprint.
  - **Polars Backend:** Employs the high-performance Polars DataFrame library for all data structuring and aggregation tasks.
- **Robust and Maintainable:**
  - **Robust Caching:** An intelligent caching layer (with version validation) speeds up repeated runs in serial mode.
  - **Explicit Data Contracts:** Uses Pydantic schemas to ensure data integrity at every step.
  - **Granular Error Handling:** A custom exception hierarchy allows for specific and meaningful error reporting.
  - **Centralized Logging:** Configurable logging provides clear insight into the pipeline's execution.
- **Multiple Output Formats:** Supports various output formats suitable for different downstream consumers (e.g., MessagePack, Parquet, JSON Lines).
- **Ergonomic Command-Line Interface:** A clean, self-documenting CLI built with `Typer` provides auto-generated help, progress bars, and flexible options for development and production use.

## Installation

A virtual environment is highly recommended. This project uses `pyproject.toml` for dependency management.

1.  **Clone the repository:**

    ```bash
    git clone <your-repo-url>
    cd message_pack_processor
    ```

2.  **Create and activate a virtual environment:**

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install the package in editable mode:**
    This command installs all dependencies from `pyproject.toml` and makes the `mpp-parser` command available in your shell. The `-e` flag means changes to your source code are reflected immediately.
    ```bash
    pip install -e .
    ```

## Usage

The application is run via the `mpp-parser` command-line tool.

`mpp-parser run -i exmaple/i -o exmaple/o -c exmaple/c test_replay_id --log-level DEBUG --output-format jsonl-gzip --serial`

### Full Help

For a full list of all commands and options:

```bash
mpp-parser --help
```

### Basic Run

This command processes a replay, computing all available stats by default and saving the output as a gzipped MessagePack file.

````bash
mpp-parser run <REPLAY_ID> \
    --input-dir ./data/input \
    --cache-dir ./data/cache \
    --output-dir ./data/output

### Run the Full Pipeline
To run the processor, you need to provide a unique ID for the replay and specify the input, cache, and output directories.

```bash
# Example run
mpp-parser run my-replay-001 \
    --input-dir ./path/to/your/mpk/files \
    --cache-dir ./data/cache \
    --output-dir ./data/output
````

**Common Flags:**

- `--force-reprocess`: Ignores any existing cache and re-parses all raw files.
- `--skip-on-error`: Logs errors for individual bad records but continues processing instead of halting.
- `--run-demo-aggregation`: Runs the built-in illustrative aggregation logic. Otherwise, a safe "pass-through" behavior is the default.
- `--dry-run`: Performs configuration validation and file ingestion, then reports what it found without processing any data.

### Computing Specific Stats

You can choose exactly which summary statistics to generate using the `--compute-stat` or `-s` flag.

```bash
# Compute only the damage breakdown
mpp-parser run <REPLAY_ID> ... -s damage_by_unit_def

# Compute both damage and resource stats
mpp-parser run <REPLAY_ID> ... -s damage_by_unit_def -s resources_by_team
```

### Changing Output Format

Select an output format suitable for your workflow. For example, to get a directory of Parquet files:

```bash
mpp-parser run <REPLAY_ID> ... --output-format parquet-dir
```

When running with multiple stats and `parquet-dir` format, you will get multiple output files, e.g.:
`./data/output/<REPLAY_ID>/damage_by_unit_def.parquet`
`./data/output/<REPLAY_ID>/resources_by_team.parquet`
`./data/output/<REPLAY_ID>/unaggregated.parquet`

### Listing Available Stats

To see a list of all recognized aggregations you can compute:

```bash
mpp-parser list-stats
```

### Serial Mode (for Caching & Debugging)

To enable caching, you must run in serial mode.

```bash
mpp-parser run <REPLAY_ID> ... --serial
```

### List Recognized Aspects

To see which aspect files the current schemas are configured to handle:

```bash
mpp-parser list-aspects
```

## Testing

A basic but critical test is included to ensure all configurations and schemas are consistent. This can be run using `pytest`.

1.  **Install testing dependencies:**
    ```bash
    pip install pytest
    ```
2.  **Run the tests:**
    ```bash
    pytest
    ```

## Project Structure

The project uses a standard `src`-layout for clean packaging and imports.

```
message_pack_parser/
├── pyproject.toml
├── requirements.txt
├── CHANGELOG.md
├── README.md
├── docs/
│   ├── aggregator_guide.md
│   ├── data_dictionary.md
│   └── design_document.md
├── src/
│   └── message_pack_parser/
│       ├── __init__.py
│       ├── main.py                     # CLI Entry Point & Orchestration
│       ├── logging_config.py           # Centralized Logging Setup
│       ├── config/
│       │   ├── __init__.py
│       │   ├── enums.py                # All Enum definitions
│       │   └── dynamic_config_builder.py # Builds configs from schemas
│       ├── core/
│       │   ├── __init__.py
│       │   ├── exceptions.py
│       │   ├── ingestion.py
│       │   ├── decoder.py
│       │   ├── cache_manager.py
│       │   ├── value_transformer.py
│       │   ├── dataframe_creator.py
│       │   ├── aggregator.py           # --- STAT ORCHESTRATOR ---
│       │   ├── output_generator.py
│       │   ├── output_strategies.py
│       │   └── stats/                  # --- STATS PLUGIN DIRECTORY ---
│       │       ├── __init__.py         # Dynamic registry builder
│       │       ├── types.py            # Shared types (e.g., Stat class)
│       │       ├── aggression_by_unit.py
│       │       ├── calculate_player_apm_and_focus.py
│       │       ├── combat_engagement_summary.py
│       │       ├── crisis_response_index.py
│       │       ├── damage_by_unit_def.py
│       │       ├── force_composition_timeline.py
│       │       ├── map_control_timeline.py
│       │       ├── player_collaboration.py
│       │       ├── player_economic_efficiency.py
│       │       ├── resources_by_player.py
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── aspects_raw.py          # --- SINGLE SOURCE OF TRUTH ---
│       │   └── aspects.py              # Clean Pydantic models
│       └── utils/
│           ├── __init__.py
│           └── config_validator.py
└── tests/
    ├── __init__.py
    ├── unit/
    │   ├── __init__.py
    │   └── test_aggregator.py
    ├── test_cli.py
    └── test_config_consistency.py
```
