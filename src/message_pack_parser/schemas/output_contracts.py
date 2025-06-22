# src/message_pack_parser/schemas/output_contracts.py
"""
Defines the transformation contracts for stats and unaggregated streams before
final serialization. This is the single source of truth for post-processing rules.
"""
from typing import Dict, Any

# Contracts here should match expected input of downstream consumer (tubuin-react)

# static assets, sidecars
#   defs_contract: see ingestion.py
defs_contract = {
    "transform": "to_lookup_map",
    "params": {
        "key_column": "unit_def_id",
        "value_columns": ["unit_name", "translated_human_name"]
    }
}

# Add other contracts that require this layout...
team_stats_contract = {
    "columns": {
        "frame": {
            "transform": "cast",
            "to_type": "Float64",  # type for consumer
        },
        "team_id": {
            "transform": "cast",
            "to_type": "Float64",  # type for consumer
        },
        "metal_used": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_produced": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_excess": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_received": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_sent": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_used": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_produced": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_excess": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_received": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_sent": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "damage_dealt": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "damage_received": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "units_killed": {"transform": "cast", "to_type": "Float64"},
        "units_died": {"transform": "cast", "to_type": "Float64"},
        "units_captured": {"transform": "cast", "to_type": "Float64"},
        "units_out_captured": {"transform": "cast", "to_type": "Float64"},
        "units_received": {"transform": "cast", "to_type": "Float64"},
        "units_sent": {"transform": "cast", "to_type": "Float64"},
        "current_unit_count": {"transform": "cast", "to_type": "Float64"},
        "metal_current": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_storage": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_pull": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_income": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_expense": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_share": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_Rsent": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_Rreceived": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "metal_Rexcess": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_current": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_storage": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_pull": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_income": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_expense": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_share": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_Rsent": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_Rreceived": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
        "energy_Rexcess": {"transform": "quantize","to_type": "Float64","params": {"type": "static", "scale": 0.1},},
    },
    "table_options": {"layout": "row-major-mixed"},
}


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
        "frame": {"transform": "cast", "to_type": "UInt32"},
        "unit_id": {"transform": "cast", "to_type": "UInt16"},
        "unit_def_id": {"transform": "cast", "to_type": "UInt16"},
        "team_id": {"transform": "cast", "to_type": "UInt16"},
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


damage_log_contract = {
    "columns": {
        "frame": {
            "transform": "cast",
            "to_type": "Int32",  # frame can be large, but use Int32 signed or UInt32 as needed
        },
        "victim_team_id": {
            "transform": "cast",
            "to_type": "Int16",
        },
        "attacker_team_id": {
            "transform": "cast",
            "to_type": "Int16",
            "null_encoding": -1,  # sentinel for missing attacker
        },
        "victim_unit_id": {
            "transform": "cast",
            "to_type": "Int32",  # likely IDs require 32-bit unsigned/int
        },
        "victim_def_id": {
            "transform": "cast",
            "to_type": "Int32",
        },
        "attacker_unit_id": {
            "transform": "cast",
            "to_type": "UInt16",
            "null_encoding": -1,  # sentinel for missing attacker
        },
        "attacker_def_id": {
            "transform": "cast",
            "to_type": "UInt16",
            "null_encoding": -1,  # sentinel for missing attacker
        },
        "weapon_def_id": {
            "transform": "cast",
            "to_type": "Int32",
        },
        "projectile_id": {
            "transform": "cast",
            "to_type": "Int32",
            "null_encoding": -1,
        },
        "damage": {
            "transform": "cast",
            "to_type": "Float64",  # damage is float in your schema
        },
        "is_paralyzer": {
            "transform": "cast",
            "to_type": "Boolean",
        },
        "victim_pos_x": {
            "transform": "cast",
            "to_type": "Int16",
        },
        "victim_pos_y": {
            "transform": "cast",
            "to_type": "Int16",
        },
        "victim_pos_z": {
            "transform": "cast",
            "to_type": "Int16",
        },
    },
    "table_options": {
        "layout": "columnar",
    },
}


OUTPUT_CONTRACTS: Dict[str, Dict[str, Any]] = {
    "army_value_timeline": army_value_timeline_contract,
    "unit_positions": unit_positions_contract,
    "unit_events": unit_events_contract,
    "team_stats": team_stats_contract,
    "damage_log": damage_log_contract,
    "defs": defs_contract
}
