# Developer Documentation (v3.1)

This document provides a comprehensive overview of the Message Pack Parser's architecture, data flow, key design patterns, and core mechanics.

- For a guide focused purely on adding new statistics, see `docs/aggregator_guide.md`.
- For a complete list of all available data fields and their schemas, see the `docs/data_dictionary.md`.

## 1. High-Level Architecture & Core Concepts

The application is a modular, 7-step pipeline designed for performance, robustness, and extensibility. The core orchestration logic resides in **`src/message_pack_parser/main.py`**, which manages the overall flow via the `parser` command-line entry point.

### 1.1. Execution Modes: Parallel vs. Serial

The pipeline can run in two distinct modes, controlled by the `--serial` CLI flag:

*   **Parallel Mode (Default):** For maximum performance on multi-core systems. It uses a `ProcessPoolExecutor` to run the most intensive steps (Decode, Transform, DataFrame Creation) concurrently for each aspect file.
    *   **Heuristic:** To avoid the overhead of spawning processes for tiny files, a threshold (`SERIAL_PROCESSING_THRESHOLD_BYTES`) is used. Files smaller than this are processed serially even in parallel mode.
    *   **Trade-off:** Caching is **disabled** because the parallel worker (_process_aspect_serially) combines the Decode, Transform, and DataFrame Creation steps into a single unit of work. The current caching layer is designed to save the intermediate artifact after the Decode step, which is bypassed in the fully-pipelined parallel path.

*   **Serial Mode (`--serial`):** For debugging, reproducibility, and enabling the cache. All steps are executed in a single thread. This mode is required for the caching layer to function and is ideal for development and repeated runs on the same dataset.

### 1.2. Streaming Design Pattern

To handle potentially very large aspect files with minimal memory overhead, the core processing steps are designed as a **streaming pipeline**.

*   The `decoder.py` module uses `msgpack.Unpacker` to yield one raw data record at a time from the input bytes, rather than loading the entire file's contents into a list.
*   The `value_transformer.py` module consumes this iterator, transforms one record, and yields one clean record.
*   This "iterator-in, iterator-out" pattern ensures that only a single record is held in memory at any given time during these intensive steps, allowing the application to process files that are much larger than available RAM.

### 1.3. Core Design Pattern: Schema as Single Source of Truth

A key architectural principle is avoiding configuration drift. The definition of the raw data, including all transformation rules, is centralized in one place.

*   **File:** `src/message_pack_parser/schemas/aspects_raw.py`
*   **Mechanism:** This file defines Pydantic models for the raw data. Transformation rules are embedded directly into these models using the `Field(metadata={...})` attribute.
    *   **Example (Dequantization):**
        ```python
        buildpower: int = Field(metadata={'dequantize_by': 1000.0})
        ```
    *   **Example (Enum Mapping):**
        ```python
        cmd_type_id: int = Field(metadata={'enum_map': ('cmd_name', CommandsEnum)})
        ```
*   **Dynamic Configuration Builder:** The module `src/message_pack_parser/config/dynamic_config_builder.py` runs at application startup. It introspects the `metadata` from all schemas in `aspects_raw.py` and dynamically generates the `DEQUANTIZATION_CONFIG` and `ASPECT_ENUM_MAPPINGS` dictionaries used by the transformer.
*   **Benefit:** To change a transformation rule, you only need to modify the schema definition in `aspects_raw.py`. The rest of the pipeline adapts automatically.

## 2. Detailed Pipeline Data Flow

This section details the exact inputs and outputs for each step of the pipeline.

### Step 1: File Ingestion
*   **Module:** `src/core/ingestion.py` (`load_mpk_files`)
*   **Input:** `List[str]` (Directory paths from the CLI).
*   **Process:** Scans directories for `.mpk` files and reads their content.
*   **Output:** `Dict[str, bytes]` (A dictionary mapping aspect names to their raw binary content).

### Step 2: Decoding
*   **Module:** `src/core/decoder.py` (`stream_decode_aspect`)
*   **Input:** `bytes` (The raw binary content of a single aspect file).
*   **Process:** Uses `msgpack.Unpacker` to decode the binary data. Each resulting record (a `list` of values) is validated and converted into a Pydantic model instance using the schemas from `aspects_raw.py`.
*   **Output:** `Iterator[BaseAspectDataPointRaw]` (A stream of Pydantic models representing the validated raw data).

### Step 3: Caching
*   **Module:** `src/core/cache_manager.py`
*   **Functionality:** This step is only active in **Serial Mode**.
*   **Save Process:**
    *   **Input:** The fully consumed stream from Step 2, collected into a `Dict[str, List[BaseAspectDataPointRaw]]`.
    *   **Process:** Generates a hash of all critical source files (`schemas`, `core` logic) to create a `pipeline_version`. The Pydantic models are serialized to dictionaries and packed into a versioned MessagePack file.
    *   **Output:** A `.mpkcache` file on disk.
*   **Load Process:**
    *   **Input:** `replay_id` and `cache_dir`.
    *   **Process:** Reads the cache file, compares its `pipeline_version` hash with the current one. If they match, it deserializes the data back into Pydantic raw models.
    *   **Output:** `Dict[str, List[BaseAspectDataPointRaw]]` or `None` if the cache is stale, missing, or corrupt.

### Step 4: Value Transformation
*   **Module:** `src/core/value_transformer.py` (`stream_transform_aspect`)
*   **Input:** `Iterator[BaseAspectDataPointRaw]` (The stream from Step 2).
*   **Process:** Consumes one raw Pydantic model at a time. It uses the dynamically generated configuration to dequantize values and map integer IDs to `Enum` members. The resulting dictionary is then validated against the corresponding "clean" schema.
*   **Output:** `Iterator[BaseAspectDataPoint]` (A stream of Pydantic models representing the clean, analytically-ready data).

### Step 5: DataFrame Creation
*   **Module:** `src/core/dataframe_creator.py` (`create_polars_dataframe_for_aspect`)
*   **Input:** `List[BaseAspectDataPoint]` (The fully consumed stream from Step 4).
*   **Process:**
    1.  Derives an explicit Polars schema (e.g., `{'frame': pl.Int64, 'cmd_name': pl.Categorical}`) from the Pydantic clean model's type hints.
    2.  Converts the `Enum` members in the Pydantic models to their string names (e.g., `<CommandsEnum.MOVE: 8>` becomes `"MOVE"`).
    3.  Loads the resulting list of dictionaries into a Polars DataFrame using the explicit schema for type safety.
*   **Output:** `pl.DataFrame`.

### Step 6: Aggregation
*   **Modules:** `src/core/aggregator.py` (Orchestrator) and the `src/core/stats/` package (Implementations).
*   **Process:** This step follows a "plugin" architecture:
    1.  The main `aggregator.py` module acts as an orchestrator. It contains no statistical logic itself.
    2.  At startup, it dynamically discovers and imports all stat modules from the `src/core/stats/` package.
    3.  Each module in the `stats` package defines one or more `Stat` objects, which bundle a calculation function with its metadata.
    4.  The orchestrator builds a `STATS_REGISTRY` from all discovered stats.
    5.  Based on user input (or defaults), it calls the appropriate functions from the registry, passing the full dictionary of DataFrames to them.
*   **Output:** `Tuple[Dict[str, pl.DataFrame], pl.DataFrame]` (a dictionary of all computed aggregated stats, and a single unaggregated DataFrame).

### Step 7: Final Output Generation
*   **Module:** `src/core/output_generator.py` and `src/core/output_strategies.py`
*   **Input:** The `Tuple` from Step 6, an output format choice from the CLI, and paths.
*   **Process:** The `generate_output` function acts as a delegator. Based on the chosen format, it instantiates the correct strategy class and calls its `write` method. The strategy object handles the final serialization:
    *   **`MessagePackGzipStrategy`**: Converts all DataFrames to dictionaries, creates a `map` object with column information, and writes a single, nested, compressed file.
    *   **`ParquetDirectoryStrategy`**: Calls `.write_parquet()` on each DataFrame to create separate, self-describing files within a new directory.
    *   **`JsonLinesGzipStrategy`**: Calls `.write_ndjson()` on each DataFrame and compresses the output, creating separate `.jsonl.gz` files.
*   **Output:** Final file(s) on disk in the user-specified format.

## 3. Error Handling Strategy

The application uses a hierarchy of custom exceptions defined in **`src/core/exceptions.py`** to allow for granular error handling.
- `ParserError`: The base class for all application-specific errors.
- `SchemaValidationError`, `TransformationError`, etc.: Specific errors raised by different pipeline stages.

This allows the main application loop in `main.py` to catch any `ParserError`, log a critical failure message (often with a helpful suggestion, like using `--force-reprocess` for a stale cache), and exit cleanly with a non-zero status code. This distinguishes expected application failures (like invalid data) from unexpected bugs (like a `KeyError`).

## 4. How to Run and Extend

### Running the Parser
The application is exposed as a command-line tool via the `parser` command after installation. Install with `pip install -e .` and run `parser --help` for all options.

### Extending the Parser
*   **Adding a New Aspect:** Follow the schema registration pattern described in Section 1.3 and validate with `pytest`.
*   **Adding a New Aggregation Stat:** Please follow the detailed guide in **`docs/aggregator_guide.md`**.
*   **Adding a New Output Strategy:** Open `src/core/output_strategies.py`, create a new class inheriting from `OutputStrategy`, implement the `write` method, and register it in the `STRATEGY_MAP`. The CLI will discover it automatically.

## 5. Testing

The project uses the `pytest` framework. Key tests include:
*   `tests/unit/test_aggregator.py`: Tests the logical correctness of individual statistical calculations.
*   `tests/test_cli.py`: Tests the CLI validation and error handling.
*   `tests/test_config_consistency.py`: An essential integration test that runs `config_validator` to prevent schema and configuration drift.

To run the full suite, execute `pytest` from the project root.