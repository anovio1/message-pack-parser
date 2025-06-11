"""Dynamically builds transformation configs by introspecting schemas in aspects_raw.py."""
from typing import Dict, Tuple, Type
from enum import Enum
from collections import defaultdict

from message_pack_parser.schemas.aspects_raw import ASPECT_TO_RAW_SCHEMA_MAP

def build_transformation_configs() -> Tuple[Dict, Dict]:
    """Introspects Pydantic raw schemas to build transformation configs."""
    dequant_config = defaultdict(lambda: {"fields": []})
    enum_config = defaultdict(dict)
    for aspect_name, schema_class in ASPECT_TO_RAW_SCHEMA_MAP.items():
        for field_name, field_info in schema_class.model_fields.items():
            if field_info.metadata:
                if 'dequantize_by' in field_info.metadata:
                    divisor = field_info.metadata['dequantize_by']
                    dequant_config[aspect_name]['divisor'] = divisor
                    dequant_config[aspect_name]['fields'].append(field_name)
                if 'enum_map' in field_info.metadata:
                    clean_field_name, enum_class = field_info.metadata['enum_map']
                    enum_config[aspect_name][field_name] = (clean_field_name, enum_class)
    return dict(dequant_config), dict(enum_config)

DEQUANTIZATION_CONFIG, ASPECT_ENUM_MAPPINGS = build_transformation_configs()