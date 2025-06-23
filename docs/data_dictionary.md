---

### **`docs/data_dictionary.md`**

# Message Pack Parser: Data Dictionary

**Document Version:** 1.0
**Schema Version:** 3.1
**Last Updated:** 2025-06-11
**Owner:** Data Engineering Team

---

## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Versioning & Change Log](#2-versioning--change-log)
- [3. Conventions & Glossary](#3-conventions--glossary)
  - [Naming Conventions](#naming-conventions)
  - [Data Type Conventions](#data-type-conventions)
  - [Common Units](#common-units)
  - [Glossary of Key Terms](#glossary-of-key-terms)
- [4. Foreign-Key Relationships & Joins](#4-foreign-key-relationships--joins)
- [5. Data Quality & Validation Notes](#5-data-quality--validation-notes)
- [6. DataFrame Schemas](#6-dataframe-schemas)
  - [Quick Reference](#quick-reference)
  - [Detailed Schemas](#detailed-schemas)
    - [`commands_log`](#commands_log-dataframe)
    - [`construction_log`](#construction_log-dataframe)
    - [`damage_log`](#damage_log-dataframe)
    - [`map_envir_econ`](#map_envir_econ-dataframe)
    - [`start_pos`](#start_pos-dataframe)
    - [`team_stats`](#team_stats-dataframe)
    - [`unit_economy`](#unit_economy-dataframe)
    - [`unit_events`](#unit_events-dataframe)
    - [`unit_positions`](#unit_positions-dataframe)
    - [`unit_state_snapshots`](#unit_state_snapshots-dataframe)
- [7. Composite / Derived DataFrames](#7-composite--derived-dataframes)
- [8. Usage Examples](#8-usage-examples)
- [9. References](#9-references)

---

## 1. Introduction

### Purpose

This document is the **canonical source of truth** for the schemas of all clean, analysis-ready Polars DataFrames available to the aggregator module (`src/message_pack_parser/core/aggregator.py`). It is intended to be the primary reference for anyone writing new statistical functions or performing data analysis.

### Audience

The primary audience for this document includes Data Scientists, Statisticians, and Analysts responsible for developing new insights from the processed game replay data.

### Data State

The data described herein has already been processed by the pipeline. This means it has been:

- Decoded from raw MessagePack format.
- Transformed (e.g., dequantized from integers to floats).
- Validated and typed according to strict schemas.

### Source of Truth

These schemas are programmatically derived from the "clean" Pydantic models located in [`src/message_pack_parser/schemas/aspects.py`](../src/message_pack_parser/schemas/aspects.py). The Polars `DataType`s listed here are the result of a direct mapping from the Python type hints in those models.

## 2. Versioning & Change Log

- **Current Schema Version:** 3.1

| Version | Date       | Change Description                                 |
| :------ | :--------- | :------------------------------------------------- |
| `1.0`   | 2025-06-13 | Initial, complete creation of the data dictionary. |

## 3. Conventions & Glossary

### Naming Conventions

- **Columns:** All DataFrame columns follow `snake_case` (e.g., `attacker_unit_id`).
- **Identifiers:**
  - `_id`: Suffix for unique integer identifiers of an entity instance (e.g., `unit_id`, `team_id`).
  - `_def_id`: Suffix for identifiers representing a _type_ or _definition_ of an entity (e.g., the `unit_def_id` for all "Tank" units).
- **Flags:** `is_` or `has_` prefixes for boolean flags (e.g., `is_cloaked`).

### Data Type Conventions

- **Enums:** Represented as Polars `pl.Categorical` for memory efficiency. Possible values are listed in each schema's "Notes" column.
- **Text:** Represented as Polars `pl.Utf8`.
- **Numbers:** Default to `pl.Int64` for integers and `pl.Float64` for floats to prevent overflow and precision issues.
- **Type Evolution Policy:** When new enum values are added to the game (e.g., a new command type), the schemas will be updated. Downstream statistical code should be written to handle unknown categorical values gracefully (e.g., by filtering them out or grouping them into an "unknown" category) rather than assuming a fixed set of values.

### Common Units

- `frame`: An integer representing a single discrete step in the game simulation (e.g., assuming 30 frames per second). This is the primary time-series key.
- `coordinates (x, y, z)`: Standard 3D world units.
- `velocity (vx, vy, vz)`: Derived float value representing world units per time interval.

### Glossary of Key Terms

- `team_id`: A unique identifier for a single player instance in a game. Despite its name, this ID does not represent a multi-player team. It is the primary key for all player-level analysis.
- `player_id`: A unique identifier for a human player. This is not present in all logs; linkable via `team_id`.
- `unit_id`: A unique identifier for a **single, specific instance** of a unit on the map (e.g., "Raptor Tank #57"). It is consistent for the life of that unit.
- `unit_def_id`: An identifier for a **type or definition** of a unit (e.g., the ID for all "Raptor" tanks).

## 4. Foreign-Key Relationships & Joins

Understanding how to link these DataFrames is key to creating powerful statistics.

- **Primary Join Keys:** `unit_id`, `team_id`, `frame`.
- **Example Joins:**
  - **Unit Lifetime:** `unit_positions` can be joined with `unit_events` on `unit_id` to link a unit's continuous positional data with its discrete lifecycle events (creation, destruction).
  - **Damage Location:** `damage_log` can be joined with `unit_positions` on `frame` and `victim_unit_id` (as `unit_id`) to get the exact `(x, y, z)` coordinate where damage was taken.
  - **Commands and Team State:** `commands_log` can be joined with `team_stats` on `frame` and `team_id` to correlate player commands with player resource changes at that moment.

## 5. Data Quality & Validation Notes

The pipeline provides the following data guarantees that analysts can rely on when writing stats:

- **`damage_log.damage`**: Guaranteed to be a non-negative float (`>= 0`).
- **`start_pos`**: Guaranteed to contain exactly one row per `player_id`.
- **`frame`**: Guaranteed to be a non-negative integer and monotonically increasing within each time-series DataFrame.
- **Non-Null Columns:** Critical ID and time-series columns like `unit_id` in `unit_positions` or `frame` in `team_stats` are guaranteed to be non-null. Optional columns are explicitly noted in the schemas below.

## 6. DataFrame Schemas

### Quick Reference

- [`commands_log`](#commands_log-dataframe)
- [`construction_log`](#construction_log-dataframe)
- [`damage_log`](#damage_log-dataframe)
- [`map_envir_econ`](#map_envir_econ-dataframe)
- [`start_pos`](#start_pos-dataframe)
- [`team_stats`](#team_stats-dataframe)
- [`unit_economy`](#unit_economy-dataframe)
- [`unit_events`](#unit_events-dataframe)
- [`unit_positions`](#unit_positions-dataframe)
- [`unit_state_snapshots`](#unit_state_snapshots-dataframe)

---

### Detailed Schemas

#### `commands_log` DataFrame

> **Cardinality:** One row per command issued to a unit.
> This DataFrame is essential for analyzing player intent and actions (APM, strategic choices).

| Column Name      | Polars Data Type | Description                                             | Notes / Example Values                                                    |
| :--------------- | :--------------- | :------------------------------------------------------ | :------------------------------------------------------------------------ |
| `frame`          | `pl.Int64`       | The frame number when the command was issued.           | Non-null.                                                                 |
| `team_id`        | `pl.Int64`       | The ID of the team that owns the commanded unit.        | Non-null.This ID refers to a single player, not a multi-player team.      |
| `unit_id`        | `pl.Int64`       | The ID of the unit that received the command.           | Non-null.                                                                 |
| `cmd_id`         | `pl.Int64`       | A unique identifier for this specific command instance. | Non-null. Passthrough from raw data.                                      |
| `cmd_name`       | `pl.Categorical` | The type of command issued.                             | Enum. Can be null if command type was invalid. Example: `MOVE`, `ATTACK`. |
| `cmd_tag`        | `pl.Int64`       | A tag associated with the command for queueing.         | Non-null.                                                                 |
| `target_unit_id` | `pl.Int64`       | The ID of the unit being targeted by the command.       | Can be null (e.g., for a move-to-ground).                                 |
| `x`, `y`, `z`    | `pl.Int64`       | The world coordinates of the command's target.          | Non-null.                                                                 |

#### `construction_log` DataFrame

> **Cardinality:** One row per construction/assistance state change event (start, snapshot, end).
> This DataFrame tracks the progress and state of all unit construction and assistance.

| Column Name           | Polars Data Type | Description                                               | Notes / Example Values                         |
| :-------------------- | :--------------- | :-------------------------------------------------------- | :--------------------------------------------- |
| `frame`               | `pl.Int64`       | The frame number of the event.                            | Non-null.                                      |
| `event`               | `pl.Categorical` | The type of construction event.                           | Enum: `CONSTRUCTION_START`, `ASSIST_END`, etc. |
| `builder_unit_id`     | `pl.Int64`       | ID of the unit performing the construction or assistance. | Non-null.                                      |
| `builder_unit_def_id` | `pl.Int64`       | The definition ID of the building unit.                   | Non-null.                                      |
| `builder_player_id`   | `pl.Int64`       | The ID of the player who owns the builder unit.           | Non-null.                                      |
| `target_unit_id`      | `pl.Int64`       | ID of the unit being built or assisted.                   | Non-null.                                      |
| `target_unit_def_id`  | `pl.Int64`       | The definition ID of the target unit.                     | Non-null.                                      |
| `target_player_id`    | `pl.Int64`       | The ID of the player who owns the target unit.            | Can be null.                                   |
| `buildpower`          | `pl.Float64`     | The amount of build power being applied at that moment.   | Dequantized float. Non-null.                   |

#### `damage_log` DataFrame

> **Cardinality:** One row per distinct damage event.
> This is a critical log for all combat analysis.

| Column Name        | Polars Data Type | Description                                      | Notes / Example Values                                                                                |
| :----------------- | :--------------- | :----------------------------------------------- | :---------------------------------------------------------------------------------------------------- |
| `frame`            | `pl.Int64`       | The frame number when damage was applied.        | Non-null.                                                                                             |
| `victim_team_id`   | `pl.Int64`       | The ID of the team that owns the damaged unit.   | Non-null. This ID refers to a single player, not a multi-player team.                                 |
| `attacker_team_id` | `pl.Int64`       | The ID of the team that owns the attacking unit. | Can be null (e.g., environmental damage). This ID refers to a single player, not a multi-player team. |
| `victim_unit_id`   | `pl.Int64`       | The ID of the unit that took damage.             | Non-null.                                                                                             |
| `victim_def_id`    | `pl.Int64`       | The definition ID of the damaged unit.           | Non-null.                                                                                             |
| `attacker_unit_id` | `pl.Int64`       | The ID of the unit that dealt the damage.        | Can be null.                                                                                          |
| `attacker_def_id`  | `pl.Int64`       | The definition ID of the attacking unit.         | Can be null.                                                                                          |
| `weapon_def_id`    | `pl.Int64`       | The definition ID of the weapon used.            | Non-null.                                                                                             |
| `projectile_id`    | `pl.Int64`       | A unique ID for the projectile instance.         | Non-null.                                                                                             |
| `damage`           | `pl.Float64`     | The amount of damage dealt.                      | Non-null.                                                                                             |
| `is_paralyzer`     | `pl.Boolean`     | True if the damage caused paralysis.             | Non-null.                                                                                             |
| `victim_pos_x`     | `pl.Int64`       | The x-coordinate of the victim when hit.         | Non-null.                                                                                             |
| `victim_pos_y`     | `pl.Int64`       | The y-coordinate of the victim when hit.         | Non-null.                                                                                             |
| `victim_pos_z`     | `pl.Int64`       | The z-coordinate of the victim when hit.         | Non-null.                                                                                             |

#### `map_envir_econ` DataFrame

> **Cardinality:** One row per frame.
> A time-series log of environmental map factors.

| Column Name      | Polars Data Type | Description                       | Notes / Example Values |
| :--------------- | :--------------- | :-------------------------------- | :--------------------- |
| `frame`          | `pl.Int64`       | The frame number of the snapshot. | Non-null.              |
| `wind_strength`  | `pl.Int64`       | Strength of the wind on the map.  | Non-null.              |
| `tidal_strength` | `pl.Int64`       | Strength of the tide on the map.  | Non-null.              |

#### `start_pos` DataFrame

> **Cardinality:** One row per player in the game.
> This is a static table providing initial player and commander information.

| Column Name          | Polars Data Type | Description                                     | Notes / Example Values |
| :------------------- | :--------------- | :---------------------------------------------- | :--------------------- |
| `player_id`          | `pl.Int64`       | The unique ID of the player. Matches Team ID    | Non-null. Primary Key. |
| `player_name`        | `pl.Utf8`        | The player's display name.                      | Non-null.              |
| `commander_def_name` | `pl.Utf8`        | The string name of the commander unit selected. | Non-null.              |
| `unit_def_id`        | `pl.Int64`       | The definition ID of the commander unit.        | Non-null.              |
| `x`, `y`, `z`        | `pl.Int64`       | The world coordinates of the player's start.    | Non-null.              |

#### `team_stats` DataFrame

> **Cardinality:** One row per team, per frame.
> A dense time-series of team-wide economic and military statistics. All resource values are dequantized floats.

| Column Name          | Polars Data Type | Description                                                                                           |
| :------------------- | :--------------- | :---------------------------------------------------------------------------------------------------- |
| `frame`              | `pl.Int64`       | The frame number of the stats snapshot.                                                               |
| `team_id`            | `pl.Int64`       | The ID of the team these stats belong to. This ID refers to a single player, not a multi-player team. |
| `metal_used`         | `pl.Float64`     | Cumulative metal consumed by the team.                                                                |
| `metal_produced`     | `pl.Float64`     | Cumulative metal produced by the team.                                                                |
| `metal_excess`       | `pl.Float64`     | Cumulative metal wasted due to full storage.                                                          |
| `metal_received`     | `pl.Float64`     | Cumulative metal received from allies.                                                                |
| `metal_sent`         | `pl.Float64`     | Cumulative metal sent to allies.                                                                      |
| `energy_used`        | `pl.Float64`     | Cumulative energy consumed by the team.                                                               |
| `energy_produced`    | `pl.Float64`     | Cumulative energy produced by the team.                                                               |
| `energy_excess`      | `pl.Float64`     | Cumulative energy wasted due to full storage.                                                         |
| `energy_received`    | `pl.Float64`     | Cumulative energy received from allies.                                                               |
| `energy_sent`        | `pl.Float64`     | Cumulative energy sent to allies.                                                                     |
| `damage_dealt`       | `pl.Float64`     | Cumulative damage dealt by the team.                                                                  |
| `damage_received`    | `pl.Float64`     | Cumulative damage received by the team.                                                               |
| `units_killed`       | `pl.Int64`       | Cumulative count of enemy units killed.                                                               |
| `units_died`         | `pl.Int64`       | Cumulative count of units lost by this team.                                                          |
| `units_captured`     | `pl.Int64`       | Cumulative count of enemy units captured by this team.                                                |
| `units_out_captured` | `pl.Int64`       | Cumulative count of units this team has had captured by an enemy.                                     |
| `units_received`     | `pl.Int64`       | Cumulative count of units received from allies.                                                       |
| `units_sent`         | `pl.Int64`       | Cumulative count of units sent to allies.                                                             |
| `max_units`          | `pl.Int64`       | The team's unit cap.                                                                                  |
| `current_unit_count` | `pl.Int64`       | The number of units the team currently controls.                                                      |
| `metal_current`      | `pl.Float64`     | The team's current metal stockpile.                                                                   |
| `metal_storage`      | `pl.Float64`     | The team's current maximum metal storage.                                                             |
| `metal_pull`         | `pl.Float64`     | The current pull on metal resources.                                                                  |
| `metal_income`       | `pl.Float64`     | The team's current metal income rate.                                                                 |
| `metal_expense`      | `pl.Float64`     | The team's current metal expense rate.                                                                |
| `metal_share`        | `pl.Float64`     | The percentage of resources shared with allies.                                                       |
| `metal_Rsent`        | `pl.Float64`     | The rate at which metal is being sent.                                                                |
| `metal_Rreceived`    | `pl.Float64`     | The rate at which metal is being received.                                                            |
| `metal_Rexcess`      | `pl.Float64`     | The rate at which metal is being wasted.                                                              |
| `energy_current`     | `pl.Float64`     | The team's current energy stockpile.                                                                  |
| `energy_storage`     | `pl.Float64`     | The team's current maximum energy storage.                                                            |
| `energy_pull`        | `pl.Float64`     | The current pull on energy resources.                                                                 |
| `energy_income`      | `pl.Float64`     | The team's current energy income rate.                                                                |
| `energy_expense`     | `pl.Float64`     | The team's current energy expense rate.                                                               |
| `energy_share`       | `pl.Float64`     | The percentage of energy shared with allies.                                                          |
| `energy_Rsent`       | `pl.Float64`     | The rate at which energy is being sent.                                                               |
| `energy_Rreceived`   | `pl.Float64`     | The rate at which energy is being received.                                                           |
| `energy_Rexcess`     | `pl.Float64`     | The rate at which energy is being wasted.                                                             |

#### `unit_economy` DataFrame

> **Cardinality:** One row per unit production event (start, snapshot, destroyed).
> Tracks the resource contribution/consumption of individual production units.

| Column Name   | Polars Data Type | Description                                                 | Notes / Example Values                                                |
| :------------ | :--------------- | :---------------------------------------------------------- | :-------------------------------------------------------------------- |
| `frame`       | `pl.Int64`       | The frame number of the economic event.                     | Non-null.                                                             |
| `unit_id`     | `pl.Int64`       | The ID of the production unit.                              | Non-null.                                                             |
| `unit_def_id` | `pl.Int64`       | The definition ID of the production unit.                   | Non-null.                                                             |
| `team_id`     | `pl.Int64`       | The ID of the team owning the unit.                         | Non-null. This ID refers to a single player, not a multi-player team. |
| `event_type`  | `pl.Categorical` | The type of economic event.                                 | Enum: `PRODUCTION_STARTED`, `SNAPSHOT`, `DESTROYED`.                  |
| `metal_make`  | `pl.Float64`     | The rate of metal production for this unit at this frame.   | Dequantized float.                                                    |
| `metal_use`   | `pl.Float64`     | The rate of metal consumption for this unit at this frame.  | Dequantized float.                                                    |
| `energy_make` | `pl.Float64`     | The rate of energy production for this unit at this frame.  | Dequantized float.                                                    |
| `energy_use`  | `pl.Float64`     | The rate of energy consumption for this unit at this frame. | Dequantized float.                                                    |

#### `unit_events` DataFrame

> **Cardinality:** One row per major unit lifecycle event.
> This is a sparse log that captures creation, completion, destruction, and change of ownership.

| Column Name            | Polars Data Type | Description                                       | Notes / Example Values                                                                          |
| :--------------------- | :--------------- | :------------------------------------------------ | :---------------------------------------------------------------------------------------------- |
| `frame`                | `pl.Int64`       | The frame number of the event.                    | Non-null.                                                                                       |
| `unit_id`              | `pl.Int64`       | The ID of the unit the event pertains to.         | Non-null.                                                                                       |
| `unit_def_id`          | `pl.Int64`       | The definition ID of the unit.                    | Can be null on some events.                                                                     |
| `unit_team_id`         | `pl.Int64`       | The team ID of the unit at the time of the event. | Non-null. This ID refers to a single player, not a multi-player team.                           |
| `x`, `y`, `z`          | `pl.Int64`       | The world coordinates where the event occurred.   | `z` can be null.                                                                                |
| `attacker_unit_id`     | `pl.Int64`       | ID of the unit that caused a `DESTROYED` event.   | Can be null. Only for `DESTROYED`.                                                              |
| `attacker_unit_def_id` | `pl.Int64`       | The definition ID of the attacking unit.          | Can be null. Only for `DESTROYED`.                                                              |
| `attacker_team_id`     | `pl.Int64`       | The team ID of the attacking unit.                | Can be null. Only for `DESTROYED`.                                                              |
| `event_type`           | `pl.Categorical` | The type of lifecycle event.                      | Enum: `CREATED`, `FINISHED`, `DESTROYED`, `GIVEN`, `TAKEN`.                                     |
| `old_team_id`          | `pl.Int64`       | The unit's previous team ID.                      | Only for `GIVEN` or `TAKEN` events. This ID refers to a single player, not a multi-player team. |
| `new_team_id`          | `pl.Int64`       | The unit's new team ID.                           | Only for `GIVEN` or `TAKEN` events. This ID refers to a single player, not a multi-player team. |
| `builder_id`           | `pl.Int64`       | ID of the unit that built this one.               | Can be null.                                                                                    |
| `factory_queue_len`    | `pl.Int64`       | Length of the factory's build queue at creation.  | Can be null.                                                                                    |

#### `unit_positions` DataFrame

> **Cardinality:** One row per unit, per frame (for units that are active).
> A dense time-series tracking the exact position and velocity of every unit on the map.

| Column Name      | Polars Data Type | Description                                 | Notes / Example Values                                                |
| :--------------- | :--------------- | :------------------------------------------ | :-------------------------------------------------------------------- |
| `frame`          | `pl.Int64`       | The frame number of the position snapshot.  | Non-null.                                                             |
| `unit_id`        | `pl.Int64`       | The unique ID of the unit.                  | Non-null.                                                             |
| `unit_def_id`    | `pl.Int64`       | The definition ID of the unit.              | Non-null.                                                             |
| `team_id`        | `pl.Int64`       | The ID of the team owning the unit.         | Non-null. This ID refers to a single player, not a multi-player team. |
| `x`, `y`, `z`    | `pl.Int64`       | The unit's world coordinates at this frame. | Non-null.                                                             |
| `vx`, `vy`, `vz` | `pl.Float64`     | The unit's velocity vector at this frame.   | Dequantized float. Non-null.                                          |
| `heading`        | `pl.Int64`       | The unit's orientation/heading (0-65535).   | Non-null.                                                             |

#### `unit_state_snapshots` DataFrame

> **Cardinality:** One row per unit, per frame (for units with state changes).
> A dense time-series tracking health, experience, and status effects for units.

| Column Name             | Polars Data Type | Description                                              | Notes / Example Values                                                |
| :---------------------- | :--------------- | :------------------------------------------------------- | :-------------------------------------------------------------------- |
| `frame`                 | `pl.Int64`       | The frame number of the state snapshot.                  | Non-null.                                                             |
| `unit_id`               | `pl.Int64`       | The unique ID of the unit.                               | Non-null.                                                             |
| `team_id`               | `pl.Int64`       | The team ID of the unit.                                 | Non-null. This ID refers to a single player, not a multi-player team. |
| `currentHealth`         | `pl.Int64`       | The unit's current health points.                        | Non-null.                                                             |
| `currentMaxHealth`      | `pl.Int64`       | The unit's maximum health points at this frame.          | Non-null.                                                             |
| `experience`            | `pl.Float64`     | The unit's experience/veterancy level.                   | Dequantized float. Non-null.                                          |
| `is_being_built`        | `pl.Boolean`     | True if the unit is currently under construction.        | Non-null.                                                             |
| `is_stunned`            | `pl.Boolean`     | True if the unit is currently stunned/paralyzed.         | Non-null.                                                             |
| `is_cloaked`            | `pl.Boolean`     | True if the unit is currently cloaked.                   | Non-null.                                                             |
| `is_transporting_count` | `pl.Int64`       | The number of units this unit is currently transporting. | Non-null.                                                             |
| `current_max_range`     | `pl.Int64`       | The current maximum weapon range of the unit.            | Non-null.                                                             |
| `is_firing`             | `pl.Boolean`     | True if the unit is currently firing its weapons.        | Non-null.                                                             |

## 7. Composite / Derived DataFrames

This section documents the schemas of the final DataFrames produced by the statistical functions in the **`STATS_REGISTRY`**. These are derived, summary tables, not direct representations of the raw aspect data. Each statistic is now implemented in its own module within the `src/message_pack_parser/core/stats/` directory.

### `aggression_by_unit`

> **Function:** [`_calculate_aggression_by_unit`](../src/message_pack_parser/core/stats/aggression_by_unit.py)
>
> **Cardinality:** One row per `unit_id` that has position data.
>
> This DataFrame provides a summary score for each unit based on its movement over time relative to its start position. It is designed to identify units that make aggressive, early-game moves.

| Column Name        | Polars Data Type | Description                                                                         | Notes / Example Values                                |
| :----------------- | :--------------- | :---------------------------------------------------------------------------------- | :---------------------------------------------------- |
| `unit_id`          | `pl.Int64`       | The unique ID of the unit being scored.                                             | Non-null. Primary Key.                                |
| `aggression_score` | `pl.Float64`     | A composite score representing total distance-over-time. Higher is more aggressive. | Non-null. The result of summing `aggression_impulse`. |

This section documents the schemas of the final DataFrames produced by the statistical functions in the `STATS_REGISTRY`. These are derived, summary tables, not direct representations of the raw aspect data. Each statistic is implemented in its own module within the [`src/message_pack_parser/core/stats/`](../src/message_pack_parser/core/stats/) directory.

### `player_economic_efficiency`

> **Source File:** [`.../stats/player_economic_efficiency.py`](../src/message_pack_parser/core/stats/player_economic_efficiency.py)
>
> **Cardinality:** One row per `player_id`.
>
> An end-of-game economic scorecard measuring a player's return on investment by comparing their total damage output to their total resource expenditure.

| Column Name                | Polars Data Type | Description                                               |
| :------------------------- | :--------------- | :-------------------------------------------------------- |
| `player_id`                | `pl.Int64`       | The unique ID of the player.                              |
| `total_metal_spent`        | `pl.Float64`     | Total metal consumed by the player throughout the game.   |
| `total_energy_spent`       | `pl.Float64`     | Total energy consumed by the player throughout the game.  |
| `total_damage_output`      | `pl.Float64`     | Total damage dealt by the player throughout the game.     |
| `damage_per_resource_unit` | `pl.Float64`     | A derived efficiency metric: `damage / (metal + energy)`. |

### `force_composition_timeline`

> **Source File:** [`.../stats/force_composition_timeline.py`](../src/message_pack_parser/core/stats/force_composition_timeline.py)
>
> **Cardinality:** One row per `player_id` per `minute`.
>
> A time-series showing a player's production choices. Each row contains a list of structs, where each struct details a unit type produced and the count for that minute.

| Column Name  | Polars Data Type                                                             | Description                                                              |
| :----------- | :--------------------------------------------------------------------------- | :----------------------------------------------------------------------- |
| `player_id`  | `pl.Int64`                                                                   | The unique ID of the player.                                             |
| `minute`     | `pl.Int32`                                                                   | The minute of the game when the units were produced.                     |
| `unit_stats` | `pl.List(pl.Struct({'unit_def_id': pl.Int64, 'units_produced': pl.UInt32}))` | A list containing each unit type and the number produced in that minute. |

### `player_apm_and_focus`

> **Source File:** [`.../stats/player_apm_and_focus.py`](../src/message_pack_parser/core/stats/player_apm_and_focus.py)
>
> **Cardinality:** One row per `player_id` per `minute`.
>
> A time-series analysis of player activity, breaking down their actions per minute (APM) and their focus on combat vs. economic tasks.

| Column Name          | Polars Data Type | Description                                                       |
| :------------------- | :--------------- | :---------------------------------------------------------------- |
| `player_id`          | `pl.Int64`       | The unique ID of the player.                                      |
| `minute`             | `pl.Int32`       | The minute of the game.                                           |
| `actions_per_minute` | `pl.UInt32`      | The total number of commands issued by the player in that minute. |
| `combat_focus_pct`   | `pl.Float64`     | The percentage of commands that were combat-oriented.             |
| `economy_focus_pct`  | `pl.Float64`     | The percentage of commands that were economy-oriented.            |

### `combat_engagement_summary`

> **Source File:** [`.../stats/combat_engagement_summary.py`](../src/message_pack_parser/core/stats/combat_engagement_summary.py)
>
> **Cardinality:** One row per discrete combat engagement.
>
> This DataFrame clusters damage events to identify distinct battles, providing details on their duration, location, participants, and intensity.

| Column Name                | Polars Data Type    | Description                                                          |
| :------------------------- | :------------------ | :------------------------------------------------------------------- |
| `engagement_id`            | `pl.UInt32`         | A unique ID for the battle instance.                                 |
| `start_frame`              | `pl.Int64`          | The frame number when the first damage event of the battle occurred. |
| `end_frame`                | `pl.Int64`          | The frame number when the last damage event of the battle occurred.  |
| `duration_seconds`         | `pl.Float64`        | The duration of the engagement in seconds.                           |
| `location_x`, `location_y` | `pl.Float64`        | The average world coordinates (centroid) of the battle.              |
| `involved_players`         | `pl.List(pl.Int64)` | A list of `player_id`s who had units dealing damage in the battle.   |
| `total_damage`             | `pl.Float64`        | The sum of all damage dealt during the engagement.                   |
| `engagement_type`          | `pl.Categorical`    | Classification of the battle (e.g., `major_battle`, `skirmish`).     |

### `map_control_timeline`

> **Source File:** [`.../stats/map_control_timeline.py`](../src/message_pack_parser/core/stats/map_control_timeline.py)
>
> **Cardinality:** One row per `player_id` per `minute`.
>
> A time-series analysis of a player's positional strategy, showing how much of the map they occupy and how dispersed their forces are.

| Column Name         | Polars Data Type | Description                                                         |
| :------------------ | :--------------- | :------------------------------------------------------------------ |
| `player_id`         | `pl.Int64`       | The unique ID of the player.                                        |
| `minute`            | `pl.Int32`       | The minute of the game.                                             |
| `bounding_box_area` | `pl.Float64`     | The area of the rectangle enclosing all of the player's units.      |
| `dispersion_x`      | `pl.Float64`     | The standard deviation of unit x-coordinates (a measure of spread). |
| `dispersion_y`      | `pl.Float64`     | The standard deviation of unit y-coordinates (a measure of spread). |

### `player_collaboration`

> **Source File:** [`.../stats/player_collaboration.py`](../src/message_pack_parser/core/stats/player_collaboration.py)
>
> **Cardinality:** One row per `player_id`.
>
> An end-of-game summary of a player's resource sharing activity, identifying net donors and receivers of both metal and energy.

| Column Name               | Polars Data Type | Description                                                   |
| :------------------------ | :--------------- | :------------------------------------------------------------ |
| `player_id`               | `pl.Int64`       | The unique ID of the player.                                  |
| `total_metal_sent`        | `pl.Float64`     | Total metal sent to allies.                                   |
| `total_metal_received`    | `pl.Float64`     | Total metal received from allies.                             |
| `total_energy_sent`       | `pl.Float64`     | Total energy sent to allies.                                  |
| `total_energy_received`   | `pl.Float64`     | Total energy received from allies.                            |
| `net_metal_contribution`  | `pl.Float64`     | Net resource change: `sent - received` (positive for donors). |
| `net_energy_contribution` | `pl.Float64`     | Net resource change: `sent - received` (positive for donors). |

### `crisis_response_index`

> **Source File:** [`.../stats/crisis_response_index.py`](../src/message_pack_parser/core/stats/crisis_response_index.py)
>
> **Cardinality:** One row per "crisis event" (a sustained attack on a player's unit).
>
> An advanced behavioral metric that measures how quickly and effectively a player responds to direct threats against their assets.

| Column Name               | Polars Data Type | Description                                                                                                            |
| :------------------------ | :--------------- | :--------------------------------------------------------------------------------------------------------------------- |
| `player_id`               | `pl.Int64`       | The ID of the player whose asset is under attack.                                                                      |
| `crisis_id`               | `pl.UInt32`      | A unique global ID for the crisis event.                                                                               |
| `victim_unit_id`          | `pl.Int64`       | The ID of the unit being attacked.                                                                                     |
| `crisis_start_frame`      | `pl.Int64`       | The frame number when the attack sequence began.                                                                       |
| `time_to_response_frames` | `pl.Int64`       | The number of frames between the crisis start and the player's first meaningful command. Value is `-1` if no response. |
| `asset_survived`          | `pl.Boolean`     | `True` if the attacked unit did not die within the response window.                                                    |

### `damage_by_unit_def`

> **Function:** [`_calculate_damage_by_unit_def`](../src/message_pack_parser/core/stats/damage_by_unit_def.py)
>
> | Cardinality: One row per unique `unit_def_id` that dealt damage.
>
> This DataFrame aggregates the total damage dealt by each type of unit, providing a clear view of which unit definitions are most effective in combat.

| Column Name          | Polars Data Type | Description                                                       | Notes / Example Values                   |
| :------------------- | :--------------- | :---------------------------------------------------------------- | :--------------------------------------- |
| `unit_def_id`        | `pl.Int64`       | The definition ID of the unit type.                               | Non-null. Primary Key.                   |
| `total_damage_dealt` | `pl.Float64`     | The sum of all damage dealt by all units of this definition type. | Dequantized float. Example: `150250.75`. |

### `resources_by_player`

> **Function:** [`_calculate_resources_by_player`](../src/message_pack_parser/core/stats/resources_by_player.py)
>
> **Cardinality:** One row per `player_id`.
>
> This DataFrame provides a total summary of resource production and consumption for each player over the entire game. It is an end-of-game economic scorecard.

| Column Name             | Polars Data Type | Description                                                             | Notes / Example Values |
| :---------------------- | :--------------- | :---------------------------------------------------------------------- | :--------------------- |
| `player_id`             | `pl.Int64`       | The unique ID of the player (renamed from the source `team_id`).        | Non-null. Primary Key. |
| `total_metal_produced`  | `pl.Float64`     | The sum total of all metal produced by the player throughout the game.  | Dequantized float.     |
| `total_metal_used`      | `pl.Float64`     | The sum total of all metal consumed by the player throughout the game.  | Dequantized float.     |
| `total_energy_produced` | `pl.Float64`     | The sum total of all energy produced by the player throughout the game. | Dequantized float.     |
| `total_energy_used`     | `pl.Float64`     | The sum total of all energy consumed by the player throughout the game. | Dequantized float.     |
---

### `unit_economic_contribution_binned`

> **Source File:** [`unit_economic_contribution_binned.py`](../src/message_pack_parser/core/stats/unit_economic_contribution_binned.py)
>
> **Cardinality:** One row per (`team_id`, `unit_def_id`, `time_bin_start_frame`) combination where economic activity or unit production occurred.
>
> A dynamic, time-series analysis of economic performance. This stat breaks down the game into discrete time intervals (bins, default 10 seconds) and calculates the total resources produced and consumed by each unit type for each player within those bins. It provides a granular view of how a player's economy evolves, highlighting production ramps, resource drains, and the shifting roles of units throughout the match.

| Column Name | Polars Data Type | Description |
| :--- | :--- | :--- |
| `team_id` | `pl.Int64` | The unique ID of the player. |
| `unit_def_id` | `pl.Int64` | The definition ID of the unit type. |
| `time_bin_start_frame` | `pl.Int64` | The starting frame of the time bin (e.g., 0, 300, 600...). |
| `units_alive_in_bin` | `pl.Int64` | The number of distinct units of this type that were active at any point during this bin. |
| `total_unit_seconds_in_bin` | `pl.Float64` | The total "unit-seconds" of lifetime for this unit type within this bin. This is the sum of the actual time (in seconds) that each unit of this type was active during the bin. |
| `total_units_produced_in_bin` | `pl.Int64` | The number of new units of this type whose creation event occurred within this bin. |
| `total_metal_produced_in_bin` | `pl.Float64` | The total amount of metal produced by all units of this type during this bin. |
| `total_metal_consumed_in_bin` | `pl.Float64` | The total amount of metal consumed by all units of this type during this bin. |
| `total_energy_produced_in_bin` | `pl.Float64` | The total amount of energy produced by all units of this type during this bin. (Column is only present if source data is available). |
| `total_energy_consumed_in_bin` | `pl.Float64` | The total amount of energy consumed by all units of this type during this bin. (Column is only present if source data is available). |
| `time_bin_end_frame` | `pl.Int64` | **(Optional)** The exclusive end frame of the time bin (e.g., 300, 600, 900...). Included for debugging if `INCLUDE_DEBUG_COLS` is set to `True`. |

## 8. Usage Examples

A few simple Polars code snippets to demonstrate common operations on the `dataframes_by_aspect` dictionary.

**Example: Get all move commands from `commands_log`**

```python
# Inside a stat calculation function in aggregator.py
commands_df = dataframes_by_aspect.get("commands_log")
if commands_df:
    move_commands = commands_df.filter(pl.col("cmd_name") == "MOVE")
```

**Example: Join damage events with unit definition**

```python
# Inside a stat calculation function in aggregator.py
damage_df = dataframes_by_aspect.get("damage_log")
events_df = dataframes_by_aspect.get("unit_events")

# Get the unit type (def_id) for each attacker
if damage_df and events_df:
    damage_with_type = damage_df.join(
        events_df.select(["unit_id", "unit_def_id"]),
        left_on="attacker_unit_id",
        right_on="unit_id",
        how="left" # Use left join to keep damage from non-unit sources
    )
```

## 9. References

- **This Document's Location:** `docs/data_dictionary.md`
- **Guide for Implementation:** For details on how to write new statistical functions using this data, see [`docs/aggregator_guide.md`](./aggregator_guide.md).
- **Source Code Schemas:** The authoritative Pydantic models are defined in [`src/message_pack_parser/schemas/aspects.py`](../src/message_pack_parser/schemas/aspects.py).
