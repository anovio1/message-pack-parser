"""
This module defines the Pydantic models for validating the RAW, post-decoded
data from MPK aspect files. It serves as the SINGLE SOURCE OF TRUTH for
transformation rules by embedding json_schema_extra directly in the field definitions.
"""

from typing import Any, Dict, List, Optional, Type
from pydantic import BaseModel, Field, ValidationError

from tubuin_processor.core.exceptions import SchemaValidationError
from tubuin_processor.config.enums import (
    CommandsEnum,
    ConstructionActionsEnum,
    UnitEconomyEventsEnum,
    UnitEventsEnum,
)


class BaseAspectDataPointRaw(BaseModel):
    @classmethod
    def from_list(
        cls: Type["BaseAspectDataPointRaw"], positional_values: List[Any]
    ) -> "BaseAspectDataPointRaw":
        field_names = list(cls.model_fields.keys())
        if len(positional_values) < len(field_names):
            positional_values.extend(
                [None] * (len(field_names) - len(positional_values))
            )
        if len(positional_values) > len(field_names):
            raise SchemaValidationError(
                f"Data Error for {cls.__name__}: Received {len(positional_values)} values, but schema defines {len(field_names)}."
            )
        raw_data_dict = dict(zip(field_names, positional_values))
        try:
            return cls.model_validate(raw_data_dict)
        except ValidationError as e:
            # --- IMPROVED ERROR HANDLING ---
            # Construct a detailed error message that includes the problematic row data.
            error_message = (
                f"Pydantic validation failed for {cls.__name__}.\n"
                f"Failing Row Data: {raw_data_dict}\n"
                f"Pydantic Errors: {e}"
            )
            raise SchemaValidationError(error_message) from e


class Commands_log_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    teamId: int
    unitId: int
    cmd_id: int
    cmd_name: Optional[int] = Field(
        default=None,
        json_schema_extra={
            "enum": [e.value for e in CommandsEnum],  # valid enum values
            "description": "Command name, mapped to CommandsEnum"
        }
    )
    cmd_tag: int
    target_unit_id: Optional[int] = None
    x: int
    y: int
    z: int


class Construction_log_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    event: int = Field(
        json_schema_extra={
            "enum": [e.value for e in ConstructionActionsEnum],  # valid enum values
            "description": "Event name, mapped to ConstructionActionsEnum"
        }
    )
    builder_unit_id: int
    builder_unit_def_id: int
    builder_player_id: int
    target_unit_id: int
    target_unit_def_id: int
    target_player_id: Optional[int] = None
    buildpower: int = Field(json_schema_extra={"dequantize_by": 1000.0})


class Team_stats_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    team_id: int
    metal_used: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_produced: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_excess: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_received: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_sent: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_used: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_produced: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_excess: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_received: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_sent: int = Field(json_schema_extra={"dequantize_by": 10.0})
    damage_dealt: int = Field(json_schema_extra={"dequantize_by": 10.0})
    damage_received: int = Field(json_schema_extra={"dequantize_by": 10.0})
    units_killed: int
    units_died: int
    units_captured: int
    units_out_captured: int
    units_received: int
    units_sent: int
    current_unit_count: int
    metal_current: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_storage: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_pull: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_income: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_expense: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_share: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_Rsent: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_Rreceived: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_Rexcess: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_current: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_storage: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_pull: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_income: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_expense: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_share: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_Rsent: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_Rreceived: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_Rexcess: int = Field(json_schema_extra={"dequantize_by": 10.0})


class Unit_economy_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    unit_id: int
    unit_def_id: int
    team_id: int
    event_type: int =  Field(
        json_schema_extra={
            "enum": [e.value for e in UnitEconomyEventsEnum],  # valid enum values
            "description": "Event type, mapped to UnitEconomyEventsEnum"
        }
    )
    metal_make: int = Field(json_schema_extra={"dequantize_by": 10.0})
    metal_use: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_make: int = Field(json_schema_extra={"dequantize_by": 10.0})
    energy_use: int = Field(json_schema_extra={"dequantize_by": 10.0})


class Unit_events_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    unit_id: int
    unit_def_id: Optional[int] = None
    unit_team_id: Optional[int] = None
    x: int
    y: int
    z: int
    attacker_unit_id: Optional[int] = None
    attacker_unit_def_id: Optional[int] = None
    attacker_team_id: Optional[int] = None
    event_type: int =  Field(
        json_schema_extra={
            "enum": [e.value for e in UnitEventsEnum],  # valid enum values
            "description": "Event type, mapped to UnitEventsEnum"
        }
    )
    old_team_id: Optional[int] = None
    new_team_id: Optional[int] = None
    builder_id: Optional[int] = None
    factory_queue_len: Optional[int] = None


class Unit_positions_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    unit_id: int
    unit_def_id: int
    team_id: int
    x: int
    y: int
    z: int
    vx: int = Field(json_schema_extra={"dequantize_by": 1000.0})
    vy: int = Field(json_schema_extra={"dequantize_by": 1000.0})
    vz: int = Field(json_schema_extra={"dequantize_by": 1000.0})
    heading: int


class Unit_state_snapshots_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    unit_id: int
    team_id: int
    currentHealth: int
    currentMaxHealth: int
    experience: int = Field(json_schema_extra={"dequantize_by": 1000.0})
    is_being_built: bool
    is_stunned: bool
    is_cloaked: bool
    is_transporting_count: int
    current_max_range: int
    is_firing: bool


class Damage_log_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    victim_team_id: int
    attacker_team_id: Optional[int] = None
    victim_unit_id: int
    victim_def_id: int
    attacker_unit_id: Optional[int] = None
    attacker_def_id: Optional[int] = None
    weapon_def_id: int
    projectile_id: int
    damage: int = Field(json_schema_extra={"dequantize_by": 10.0})
    is_paralyzer: bool
    victim_pos_x: int
    victim_pos_y: int
    victim_pos_z: int


class Map_envir_econ_Schema_Raw(BaseAspectDataPointRaw):
    frame: int
    wind_strength: int
    tidal_strength: int


class Start_pos_Schema_Raw(BaseAspectDataPointRaw):
    player_id: int
    player_name: str
    commander_def_name: str
    unit_def_id: int
    x: int
    y: int
    z: int


ASPECT_TO_RAW_SCHEMA_MAP: Dict[str, Type[BaseAspectDataPointRaw]] = {
    "commands_log": Commands_log_Schema_Raw,
    "construction_log": Construction_log_Schema_Raw,
    "damage_log": Damage_log_Schema_Raw,
    "map_envir_econ": Map_envir_econ_Schema_Raw,
    "start_pos": Start_pos_Schema_Raw,
    "team_stats": Team_stats_Schema_Raw,
    "unit_economy": Unit_economy_Schema_Raw,
    "unit_events": Unit_events_Schema_Raw,
    "unit_positions": Unit_positions_Schema_Raw,
    "unit_state_snapshots": Unit_state_snapshots_Schema_Raw,
}
