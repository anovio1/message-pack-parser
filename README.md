# Message Pack Parser

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A production-grade, parallelized pipeline for ingesting, transforming, and aggregating game replay data from `.mpk` (MessagePack) files.

This tool is designed for performance, maintainability, and robustness, converting raw, compact replay data into structured, analyzable formats.

Tested on Python 3.11

## Core Features

-   **Schema-Driven Configuration:** A single source of truth (`aspects_raw.py`) defines both the raw data structure and all transformation rules (dequantization, enum mapping), eliminating configuration drift.
-   **High-Performance Processing:**
    *   **Parallel Execution:** Leverages `ProcessPoolExecutor` to process multiple large data files concurrently, maximizing CPU usage.
    *   **Streaming Decoder:** Uses a streaming `msgpack.Unpacker` to handle arbitrarily large aspect files with minimal memory footprint.
    *   **Polars Backend:** Employs the high-performance Polars DataFrame library for all data structuring and aggregation tasks.
-   **Robust and Maintainable:**
    *   **Explicit Data Contracts:** Uses Pydantic for rigorous data validation at each stage of the pipeline.
    *   **Granular Error Handling:** A custom exception hierarchy allows for specific and meaningful error reporting.
    *   **Centralized Logging:** Configurable logging provides clear insight into the pipeline's execution.
-   **Ergonomic Command-Line Interface:** A modern CLI built with `Typer` provides auto-generated help, progress bars, and flexible options for development and production use.

## Project Structure

The project uses a standard `src`-layout for clean packaging and imports.

```
message_pack_parser/
├── pyproject.toml
├── requirements.txt
├── setup.py
├── src/
│   └── message_pack_parser/
│       ├── __init__.py
│       ├── main.py                     # CLI Entry Point
│       ├── logging_config.py
│       ├── config/
│       │   ├── enums.py
│       │   └── dynamic_config_builder.py
│       ├── core/
│       │   ├── exceptions.py
│       │   ├── ingestion.py
│       │   ├── decoder.py
│       │   ├── cache_manager.py
│       │   ├── value_transformer.py
│       │   ├── dataframe_creator.py
│       │   ├── aggregator.py
│       │   └── output_generator.py
│       ├── schemas/
│       │   ├── aspects_raw.py          # Single Source of Truth
│       │   └── aspects.py
│       └── utils/
│           └── config_validator.py
└── tests/
    └── test_config_consistency.py
```

## Installation

This project uses modern Python packaging. A virtual environment is highly recommended.

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd message_pack_parser
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

### View All Commands and Options
```bash
mpp-parser --help
```

### Run the Full Pipeline
To run the parser, you need to provide a unique ID for the replay and specify the input, cache, and output directories.

```bash
# Example run
mpp-parser run my-replay-001 \
    --input-dir ./path/to/your/mpk/files \
    --cache-dir ./data/cache \
    --output-dir ./data/output
```
**Common Flags:**
*   `--force-reprocess`: Ignores any existing cache and re-parses all raw files.
*   `--skip-on-error`: Logs errors for individual bad records but continues processing instead of halting.
*   `--run-demo-aggregation`: Runs the built-in illustrative aggregation logic. Otherwise, a safe "pass-through" behavior is the default.
*   `--dry-run`: Performs configuration validation and file ingestion, then reports what it found without processing any data.

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

## Future Work
For a detailed list of potential future enhancements, including advanced caching strategies, CI integration, and more, please see the `FutureConcerns.md` document (if available in your repository).