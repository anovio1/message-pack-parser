import logging
from message_pack_parser.schemas.aspects_raw import ASPECT_TO_RAW_SCHEMA_MAP
from message_pack_parser.schemas.aspects import ASPECT_TO_CLEAN_SCHEMA_MAP
from message_pack_parser.config.dynamic_config_builder import DEQUANTIZATION_CONFIG, ASPECT_ENUM_MAPPINGS

logger = logging.getLogger(__name__)

def validate_configurations():
    """Checks that all major config dictionaries share the same set of aspect keys."""
    raw_keys = set(ASPECT_TO_RAW_SCHEMA_MAP.keys())
    clean_keys = set(ASPECT_TO_CLEAN_SCHEMA_MAP.keys())
    dequant_keys = set(DEQUANTIZATION_CONFIG.keys())
    enum_keys = set(ASPECT_ENUM_MAPPINGS.keys())
    all_schema_keys = raw_keys

    assert raw_keys == clean_keys, f"Mismatch between raw and clean schema keys:\nOnly in raw: {raw_keys - clean_keys}\nOnly in clean: {clean_keys - raw_keys}"
    assert dequant_keys.issubset(all_schema_keys), f"Keys in DEQUANTIZATION_CONFIG not found in schemas: {dequant_keys - all_schema_keys}"
    assert enum_keys.issubset(all_schema_keys), f"Keys in ASPECT_ENUM_MAPPINGS not found in schemas: {enum_keys - all_schema_keys}"

    logger.info("Configuration validation successful: All schema and config keys are consistent.")