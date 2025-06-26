"""
Dynamically builds transformation configs by introspecting schemas in aspects_raw.py.
Uses the `json_schema_extra` attribute for metadata.
"""
from typing import Dict, Tuple, Type, Any
from enum import Enum
from collections import defaultdict

from tubuin_processor.schemas.aspects_raw import ASPECT_TO_RAW_SCHEMA_MAP
from tubuin_processor.schemas.output_contracts import OUTPUT_CONTRACTS

def build_transformation_configs() -> Tuple[Dict, Dict]:
    """
    Introspects Pydantic raw schemas to build transformation configs from `json_schema_extra`.
    """
    dequant_config = defaultdict(lambda: {"fields": []})
    enum_config = defaultdict(dict)
    for aspect_name, schema_class in ASPECT_TO_RAW_SCHEMA_MAP.items():
        for field_name, field_info in schema_class.model_fields.items():
            
            extra_schema_data: Any = field_info.json_schema_extra

            if extra_schema_data is None:
                continue
            
            # If it's a callable, call it to get the dictionary
            if callable(extra_schema_data):
                extra_schema = extra_schema_data({})
            else:
                extra_schema = extra_schema_data
                
            if isinstance(extra_schema, dict):
                if 'dequantize_by' in extra_schema:
                    divisor = extra_schema['dequantize_by']
                    dequant_config[aspect_name]['divisor'] = divisor
                    dequant_config[aspect_name]['fields'].append(field_name)
                    
                if 'enum_map' in extra_schema:
                    clean_field_name, enum_class = extra_schema['enum_map']
                    enum_config[aspect_name][field_name] = (clean_field_name, enum_class)
            
    return dict(dequant_config), dict(enum_config)

# These configurations are now built from the json_schema_extra attribute.
DEQUANTIZATION_CONFIG, ASPECT_ENUM_MAPPINGS = build_transformation_configs()

# --- Output Configs
OUTPUT_TRANSFORMATION_CONFIG: dict = OUTPUT_CONTRACTS