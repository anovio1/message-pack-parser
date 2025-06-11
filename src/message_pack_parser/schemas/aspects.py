"""Defines the Pydantic models for the FINAL, CLEAN, DE-QUANTIZED data structures."""

from typing import List, Optional, Type, Dict
from enum import Enum
import polars as pl
from pydantic import BaseModel
from message_pack_parser.config.enums import (
    CommandsEnum,
    ConstructionActionsEnum,
    UnitEventsEnum,
    UnitEconomyEventsEnum,
)

PYDANTIC_TO_POLARS_TYPE_MAP = {
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    str: pl.Utf8,
}


class Commands_log_Schema(BaseModel):
    frame: int
    teamId: int
    unitId: int
    cmd_id: int
    cmd_name: Optional[CommandsEnum]
    cmd_tag: int
    target_unit_id: Optional[int] = None
    x: int
    y: int
    z: int


class Construction_log_Schema(BaseModel):
    frame: int
    event: ConstructionActionsEnum
    builder_unit_id: int
    builder_unit_def_id: int
    builder_player_id: int
    target_unit_id: int
    target_unit_def_id: int
    target_player_id: Optional[int] = None
    buildpower: float


class Unit_economy_Schema(BaseModel):
    frame: int
    unit_id: int
    unit_def_id: int
    team_id: int
    event_type: UnitEconomyEventsEnum
    metal_make: float
    metal_use: float
    energy_make: float
    energy_use: float


class Unit_events_Schema(BaseModel):
    frame: int
    unit_id: int
    unitDefID: Optional[int] = None
    unit_team_id: Optional[int] = None
    x: int
    y: int
    z: int
    attacker_unit_id: Optional[int] = None
    attacker_unit_def_id: Optional[int] = None
    attacker_team_id: Optional[int] = None
    event_type: UnitEventsEnum
    old_team_id: Optional[int] = None
    new_team_id: Optional[int] = None
    builder_id: Optional[int] = None
    factory_queue_len: Optional[int] = None


class Damage_log_Schema(BaseModel):
    frame: int
    victim_team_id: int
    attacker_team_id: Optional[int] = None
    victim_unit_id: int
    victim_def_id: int
    attacker_unit_id: Optional[int] = None
    attacker_def_id: Optional[int] = None
    weapon_def_id: int
    projectile_id: int
    damage: int
    is_paralyzer: bool
    victim_pos_x: int
    victim_pos_y: int
    victim_pos_z: int


class Map_envir_econ_Schema(BaseModel):
    frame: int
    wind_strength: int
    tidal_strength: int


class Start_pos_Schema(BaseModel):
    player_id: int
    player_name: str
    commander_def_name: str
    unit_def_id: int
    x: int
    y: int
    z: int


class Team_stats_Schema(BaseModel):
    frame: int
    team_id: int
    metal_used: float
    metal_produced: float
    metal_excess: float
    metal_received: float
    metal_sent: float
    energy_used: float
    energy_produced: float
    energy_excess: float
    energy_received: float
    energy_sent: float
    damage_dealt: float
    damage_received: float
    units_killed: int
    units_died: int
    units_captured: int
    units_out_captured: int
    units_received: int
    units_sent: int
    max_units: int
    current_unit_count: int
    metal_current: float
    metal_storage: float
    metal_pull: float
    metal_income: float
    metal_expense: float
    metal_share: float
    metal_Rsent: float
    metal_Rreceived: float
    metal_Rexcess: float
    energy_current: float
    energy_storage: float
    energy_pull: float
    energy_income: float
    energy_expense: float
    energy_share: float
    energy_Rsent: float
    energy_Rreceived: float
    energy_Rexcess: float


class Unit_positions_Schema(BaseModel):
    frame: int
    unit_id: int
    unit_def_id: int
    team_id: int
    x: int
    y: int
    z: int
    vx: float
    vy: float
    vz: float
    heading: int


class Unit_state_snapshots_Schema(BaseModel):
    frame: int
    unit_id: int
    team_id: int
    currentHealth: int
    currentMaxHealth: int
    experience: float
    is_being_built: bool
    is_stunned: bool
    is_cloaked: bool
    is_transporting_count: int
    current_max_range: int
    is_firing: bool


ASPECT_TO_CLEAN_SCHEMA_MAP: Dict[str, Type[BaseModel]] = {
    "commands_log": Commands_log_Schema,
    "construction_log": Construction_log_Schema,
    "damage_log": Damage_log_Schema,
    "map_envir_econ": Map_envir_econ_Schema,
    "start_pos": Start_pos_Schema,
    "team_stats": Team_stats_Schema,
    "unit_economy": Unit_economy_Schema,
    "unit_events": Unit_events_Schema,
    "unit_positions": Unit_positions_Schema,
    "unit_state_snapshots": Unit_state_snapshots_Schema,
}
