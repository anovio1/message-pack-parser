# Developer Documentation: Message Pack Parser

This document provides an in-depth explanation of the Message Pack Parser's architecture, core concepts, and extension points. It is intended for developers who will be maintaining or extending the application.

## 1. Design Philosophy

The application is built on three core principles:

1.  **Single Source of Truth:** Configurations should be declared once, as close to the data they describe as possible. This prevents configuration drift and reduces the maintenance burden.
2.  **Robust Data Contracts:** The structure and type of data at every stage of the pipeline must be explicitly defined and validated. This catches errors early and makes the data flow predictable.
3.  **Performance by Design:** The architecture should be designed to handle large datasets efficiently, using streaming and parallelism where appropriate, without sacrificing clarity.

## 2. Architectural Overview: The 7-Step Pipeline

The application orchestrates a 7-step pipeline, with data transforming at each stage. This flow is managed by `src/message_pack_parser/main.py`.

-   **Step 1: Ingestion (`core/ingestion.py`)**
    -   **Input:** Directory paths (from CLI).
    -   **Process:** Loads all `.mpk` files into memory.
    -   **Output:** `Dict[str, bytes]` (A dictionary mapping aspect names to their raw binary content).

-   **Step 2: Decoding (`core/decoder.py`)**
    -   **Input:** The raw binary data from Step 1.
    -   **Process:** Uses `msgpack.Unpacker` to stream-decode the bytes. Each record is validated against its corresponding schema in `schemas/aspects_raw.py`.
    -   **Output:** A stream of Pydantic `BaseAspectDataPointRaw` model instances.

-   **Step 3: Caching (`core/cache_manager.py`)**
    -   **Input:** Decoded raw Pydantic models from Step 2.
    -   **Process:** Serializes the raw models and saves them to a versioned cache file. On subsequent runs, it attempts to load from this cache, validating the version hash to prevent stale data.
    -   **Output:** A `.mpkcache` file on disk, or a stream of Pydantic raw models loaded from it.

-   **Step 4: Value Transformation (`core/value_transformer.py`)**
    -   **Input:** A stream of raw Pydantic models.
    -   **Process:** Iterates through the stream, applying dequantization and enum mapping rules derived from the schema metadata. The transformed data is validated against "clean" schemas from `schemas/aspects.py`.
    -   **Output:** A stream of Pydantic `BaseAspectDataPoint` (clean) model instances.

-   **Step 5: DataFrame Creation (`core/dataframe_creator.py`)**
    -   **Input:** A list of clean Pydantic models (the stream from Step 4 is consumed).
    -   **Process:** Converts the list of Pydantic models into a Polars DataFrame, applying an *explicitly derived Polars schema* for type safety.
    -   **Output:** A `polars.DataFrame` object.

-   **Step 6: Aggregation (`core/aggregator.py`)**
    -   **Input:** A dictionary of DataFrames, keyed by aspect name.
    -   **Process:** This is the primary extension point for domain logic. It performs joins, aggregations, and calculations to produce meaningful summary data.
    -   **Output:** A tuple containing two DataFrames: `(aggregated_df, unaggregated_df)`.

-   **Step 7: Output Generation (`core/output_generator.py`)**
    -   **Input:** The two DataFrames from Step 6.
    -   **Process:** Converts the DataFrames to a dictionary format, combines them with a metadata `map`, serializes the entire object with MessagePack, and compresses it with Gzip.
    -   **Output:** The final `.mpk.gz` artifact.

## 3. Core Concepts & Design Patterns

### 3.1. Schema-Driven Configuration (Single Source of Truth)

This is the most important design pattern in the application. We avoid separate, hard-to-maintain config files (like the old `dequantization_config.py`) by embedding transformation rules directly into the raw Pydantic schemas.

**How it works:**
1.  **Declaration:** In `schemas/aspects_raw.py`, we use `pydantic.Field` to attach metadata to specific fields.
    ```python
    # From aspects_raw.py
    class Construction_log_Schema_Raw(BaseAspectDataPointRaw):
        event: int = Field(metadata={'enum_map': ('event', ConstructionActionsEnum)})
        buildpower: int = Field(metadata={'dequantize_by': 1000.0})
    ```
2.  **Introspection:** At startup, `config/dynamic_config_builder.py` runs. It iterates through all schemas defined in `ASPECT_TO_RAW_SCHEMA_MAP` and introspects the `metadata` of each field.
3.  **Dynamic Generation:** It populates two dictionaries, `DEQUANTIZATION_CONFIG` and `ASPECT_ENUM_MAPPINGS`, based on the metadata it finds.
4.  **Usage:** The `core/value_transformer.py` module imports and uses these dynamically generated dictionaries to apply the correct transformations.

**Benefits:**
-   To change a rule, you only edit one place: the schema definition itself.
-   It's impossible for the transformation configuration to become out of sync with the data schema.
-   The configuration is self-documenting, as it lives right next to the field it affects.

### 3.2. Streaming and Parallel Processing

To handle large datasets efficiently, the pipeline combines streaming with process-based parallelism.

-   **Streaming:** For individual large files, `core/decoder.py` uses `msgpack.Unpacker`. This reads the file in chunks and `yield`s one record at a time, preventing the entire file's decoded contents from being loaded into memory. `core/value_transformer.py` is also designed to operate on these streams (iterators).

-   **Parallelism:** For processing many aspect files at once, `main.py` uses `concurrent.futures.ProcessPoolExecutor`.
    -   The `_process_aspect_serially` function encapsulates the full pipeline for a single aspect (decode -> transform -> dataframe).
    -   A **heuristic** (`SERIAL_PROCESSING_THRESHOLD_BYTES`) is used to prevent the overhead of creating new processes for tiny files. Small files are processed in the main thread.
    -   Large files are submitted as jobs to the process pool. A progress bar from the `rich` library provides user feedback.

### 3.3. Error Handling Hierarchy

The `core/exceptions.py` file defines a set of custom exceptions that inherit from a base `ParserError`. This allows the `main.py` orchestrator to have a single, clean `try...except ParserError` block to catch any known, "expected" failure from the pipeline and exit gracefully. Unhandled `Exception`s are caught separately as a last resort, indicating a potential bug.

## 4. How to Extend the Parser

### 4.1. Adding a New Aspect
Let's say you have a new file, `unit_abilities.mpk`.

1.  **Define Raw Schema:** In `schemas/aspects_raw.py`, create a new Pydantic class:
    ```python
    class Unit_abilities_Schema_Raw(BaseAspectDataPointRaw):
        frame: int
        unit_id: int
        ability_id: int = Field(metadata={'enum_map': ('ability_name', YourNewAbilityEnum)})
        energy_cost: int = Field(metadata={'dequantize_by': 10.0})
    ```
2.  **Define Enum (if needed):** In `config/enums.py`, add your new enum:
    ```python
    class YourNewAbilityEnum(IntEnum):
        STEALTH = 1
        OVERDRIVE = 2
    ```
3.  **Define Clean Schema:** In `schemas/aspects.py`, create the corresponding "clean" model:
    ```python
    from message_pack_parser.config.enums import YourNewAbilityEnum

    class Unit_abilities_Schema(BaseModel):
        frame: int
        unit_id: int
        ability_name: YourNewAbilityEnum
        energy_cost: float
    ```
4.  **Register the Schemas:** Add your new aspect and its schema classes to the two main mapping dictionaries:
    -   In `schemas/aspects_raw.py`: Add `'unit_abilities': Unit_abilities_Schema_Raw` to `ASPECT_TO_RAW_SCHEMA_MAP`.
    -   In `schemas/aspects.py`: Add `'unit_abilities': Unit_abilities_Schema` to `ASPECT_TO_CLEAN_SCHEMA_MAP`.
5.  **Validate:** Run the consistency checker test (`pytest`) to ensure all configurations are aligned.

That's it. The dynamic config builder and the rest of the pipeline will automatically pick up the new aspect and its transformation rules.

### 4.2. Implementing Production Aggregations

The `core/aggregator.py` module is the primary place for domain-specific business logic.

1.  **Locate the `perform_aggregations` function.**
2.  Remove the `NotImplementedError` and replace the placeholder logic inside.
3.  Access the DataFrames for each aspect via the `dataframes_by_aspect` dictionary.
4.  Use Polars functions (`join`, `group_by`, `agg`, `filter`, etc.) to create your final `aggregated_df` and `unaggregated_df`.
5.  When you run the CLI, do *not* use the `--run-demo-aggregation` flag to ensure your new logic is executed. The default pass-through behavior is a safe fallback if your logic only populates one of the two output streams.

## 5. Testing and Validation

The project includes a critical pre-flight check in `utils/config_validator.py`. This script is executed at the start of every run and is also part of the test suite. It asserts that all schema and config mappings are consistent, preventing runtime errors due to configuration drift.

To maintain the project's health, it is highly recommended to:
-   Add new unit tests to `tests/` for any new utility functions.
-   Create integration tests that run the full pipeline on small, sample `.mpk` files and assert properties of the output DataFrames.