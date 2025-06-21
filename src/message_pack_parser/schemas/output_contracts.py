# src/message_pack_parser/schemas/output_contracts.py
"""
Defines the transformation contracts for stats and unaggregated streams before
final serialization. This is the single source of truth for post-processing rules.
"""
from typing import Dict, Any

# Contracts here should match expected input of downstream consumer (tubuin-react)


# Add other contracts that require this layout...
army_value_timeline_contract = {
    "columns": {
        "frame": {
            "transform": "cast",
            "to_type": "UInt32",  # type for consumer
        },
        "team_id": {
            "transform": "cast",
            "to_type": "UInt32",  # type for consumer
        },
        "army_value": {
            "transform": "quantize",
            "to_type": "UInt32",  # type for consumer
            "params": {"type": "static", "scale": 0.1},  # divide by 10 by consumer
        },
    },
    "table_options": {"layout": "row-major-mixed"},
}


unit_positions_contract = {
    "columns": {
        "x": {"transform": "cast", "to_type": "Int16"},
        "y": {"transform": "cast", "to_type": "Int16"},
        "z": {"transform": "cast", "to_type": "Int16"},
        "vx": {"transform": "cast", "to_type": "Int16"},
        "vy": {"transform": "cast", "to_type": "Int16"},
        "vz": {"transform": "cast", "to_type": "Int16"},
    },
    # This key tells the RowMajor strategy to process this stream.
    "table_options": {"layout": "row-major-mixed"},
}

unit_events_contract = {
    "columns": {
        "frame": {"transform": "cast", "to_type": "UInt32"},
        "unit_id": {"transform": "cast", "to_type": "UInt32"},
        "unit_def_id": {"transform": "cast", "to_type": "UInt32"},
        "unit_team_id": {"transform": "cast", "to_type": "UInt32"},
        "x": {"transform": "cast", "to_type": "UInt32"},
        "y": {"transform": "cast", "to_type": "UInt32"},
        "z": {"transform": "cast", "to_type": "UInt32"},
        "attacker_unit_id": {"transform": "cast", "to_type": "UInt32"},
        "attacker_unit_def_id": {"transform": "cast", "to_type": "UInt32"},
        "attacker_team_id": {"transform": "cast", "to_type": "UInt32"},
        "event_type": {
            "transform": "enum_to_int",
            "params": {"enum_key": "UnitEventsEnum"},
            "to_type": "UInt32",
        },
        "old_team_id": {"transform": "cast", "to_type": "UInt32"},
        "new_team_id": {"transform": "cast", "to_type": "UInt32"},
        "builder_id": {"transform": "cast", "to_type": "UInt32"},
        "factory_queue_len": {"transform": "cast", "to_type": "UInt32"},
    },
    # This key tells the RowMajor strategy to process this stream.
    "table_options": {"layout": "row-major-mixed", "null_encoding": 0},
}

OUTPUT_CONTRACTS: Dict[str, Dict[str, Any]] = {
    "army_value_timeline": army_value_timeline_contract,
    "unit_positions": unit_positions_contract,
    "unit_events": unit_events_contract,
}
