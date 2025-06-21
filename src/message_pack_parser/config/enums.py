"""This module contains all Enum definitions for the application."""
from enum import IntEnum
from typing import Dict, Type

class CommandsEnum(IntEnum):
    BUILD = 1; ATTACK = 2; CAPTURE = 3; FIGHT = 4; GUARD = 5; LOAD_UNITS = 6; MANUAL_FIRE = 7; MOVE = 8; PATROL = 9; RECLAIM = 10; REPAIR = 11; RESURRECT = 12; STOP = 13; UNLOAD_UNITS = 14; WAIT = 15

class ConstructionActionsEnum(IntEnum):
    CONSTRUCTION_START = 1; CONSTRUCTION_SNAPSHOT = 2; CONSTRUCTION_END = 3; ASSIST_START = 4; ASSIST_SNAPSHOT = 5; ASSIST_END = 6

class UnitEconomyEventsEnum(IntEnum):
    PRODUCTION_STARTED = 1; SNAPSHOT = 2; DESTROYED = 3

class UnitEventsEnum(IntEnum):
    CREATED = 1; FINISHED = 2; DESTROYED = 3; GIVEN = 4; TAKEN = 5

ENUM_REGISTRY: Dict[str, Type[IntEnum]] = {
    "UnitEventsEnum": UnitEventsEnum,
    "CommandsEnum": CommandsEnum,
    "ConstructionActionsEnum": ConstructionActionsEnum,
    "UnitEconomyEventsEnum": UnitEconomyEventsEnum,
    # Add any other enums that will need this string-to-int mapping in the future.
}