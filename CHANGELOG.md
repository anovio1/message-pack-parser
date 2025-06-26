# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.0] - 2025-06-11

This release introduces a fully pluggable and extensible aggregation engine, significantly refactoring the previous version for better maintainability and scalability. The `perform_aggregations` API has a breaking change.

### Added
- **Pluggable Stats Architecture:** The aggregator now dynamically discovers and registers statistics from modules within the new `src/core/stats/` package. Developers can add new analytics by simply adding new files to this directory.
- **New Statistics:** Implemented a full suite of seven advanced statistics, including `player_economic_efficiency`, `combat_engagement_summary`, and `crisis_response_index`.
- **Configurable Default Stats:** The `Stat` object now includes a `default_enabled` flag, allowing maintainers to curate which stats are run when no specific stats are requested by the user.
- **`list-stats` CLI Command:** A new command, `tube list-stats`, was added to display all available aggregation statistics and their descriptions, populated dynamically from the registry.
-   **CLI Validation:** The `--stat` option now uses a Typer callback to validate input against the registry, providing immediate feedback for typos.
-   **Comprehensive Documentation:**
    -   Added this `CHANGELOG.md`.
    -   Created a new `docs/aggregator_guide.md` explaining how to build new stat modules.
    -   Fully updated `README.md`, `docs/developer_documentation.md`, and `developer_aggregator_guide.md` to reflect the new architecture.

### Changed
-   **[BREAKING CHANGE] Architectural Refactor:** The monolithic `aggregator.py` has been refactored. The core orchestration logic remains, but all individual statistic calculation functions have been moved to their own modules within `src/core/stats/`.
-   **[BREAKING CHANGE]** The `perform_aggregations` function in `src/core/aggregator.py` now returns a dictionary of all computed statistics (`Dict[str, pl.DataFrame]`) instead of a single DataFrame.
-   **[BREAKING CHANGE]** All `OutputStrategy` classes now expect a dictionary of stats (`Dict[str, pl.DataFrame]`) and will write multiple output files/streams for formats like Parquet and JSON Lines.
-   **Default Behavior:** If no `--statstat` flags are provided, the parser now computes all stats marked as `default_enabled=True`.
-   **Pydantic Best Practice:** Updated schemas in `aspects_raw.py` to use `Field(metadata={...})` instead of the older `json_schema_extra` attribute for defining transformation rules.

---

## [3.0.0] - 2025-06-11

Major architectural refactor focused on performance, robustness, and developer experience.

### Added
- **Parallel Processing:** Introduced `ProcessPoolExecutor` to process aspects concurrently.
- **Streaming Architecture:** Refactored `decoder.py` and `value_transformer.py` to process data as streams, reducing memory usage.
- **Configurable Output Strategies:** Implemented the Strategy pattern for output formats (`--output-format`).
- **Professional CLI:** Migrated from `argparse` to `Typer`, adding a better user experience and progress bars.
- **Intelligent Caching:** Implemented cache version hashing to automatically invalidate stale caches.
- **Custom Exception Hierarchy:** Defined specific exceptions for more granular error handling.
- **Centralized Logging:** Replaced all `print()` statements with a configurable `logging` system.

### Changed
- **Project Structure:** The project was converted into a standard, installable Python package with a `pyproject.toml` and a `src/tubuin_processor` layout.
- **Single Source of Truth:** `aspects_raw.py` became the single source of truth for transformation rules, deprecating the static `dequantization_config.py`.