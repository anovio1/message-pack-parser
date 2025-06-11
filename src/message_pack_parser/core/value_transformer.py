"""Step 4: Data Value Transformation (Dequantization & Enum Mapping)"""
from typing import Iterator
import logging
from pydantic import ValidationError, BaseModel  # Import BaseModel directly

from message_pack_parser.schemas.aspects_raw import BaseAspectDataPointRaw
from message_pack_parser.schemas.aspects import ASPECT_TO_CLEAN_SCHEMA_MAP # No longer imports BaseAspectDataPoint
from message_pack_parser.config.dynamic_config_builder import DEQUANTIZATION_CONFIG, ASPECT_ENUM_MAPPINGS
from message_pack_parser.core.exceptions import TransformationError

logger = logging.getLogger(__name__)

def stream_transform_aspect(
    aspect_name: str, 
    raw_model_stream: Iterator[BaseAspectDataPointRaw], 
    skip_on_error: bool = False
) -> Iterator[BaseModel]:  # <--- CORRECTED RETURN TYPE
    """Applies transformations to a stream of raw Pydantic models."""
    clean_schema_type = ASPECT_TO_CLEAN_SCHEMA_MAP.get(aspect_name)
    if not clean_schema_type:
        logger.warning(f"No clean schema mapping for '{aspect_name}'. Skipping.")
        return
    
    dequant_rules = DEQUANTIZATION_CONFIG.get(aspect_name, {})
    enum_rules = ASPECT_ENUM_MAPPINGS.get(aspect_name, {})
    
    for i, raw_model in enumerate(raw_model_stream):
        try:
            transformed_dict = raw_model.model_dump(mode='json')
            
            if divisor := dequant_rules.get("divisor"):
                for field in dequant_rules.get("fields", []):
                    if field in transformed_dict and transformed_dict[field] is not None:
                        transformed_dict[field] /= divisor
            
            for raw_field, (clean_field, enum_class) in enum_rules.items():
                if raw_field in transformed_dict:
                    raw_val = transformed_dict.pop(raw_field) if raw_field != clean_field else transformed_dict[raw_field]
                    if raw_val is not None:
                        try:
                            transformed_dict[clean_field] = enum_class(raw_val)
                        except ValueError:
                            logger.warning(f"Invalid enum value '{raw_val}' for {raw_field} in {aspect_name} (row {i}). Setting to None.")
                            transformed_dict[clean_field] = None
            
            yield clean_schema_type.model_validate(transformed_dict)
        except (ValidationError, TypeError) as e:
            if skip_on_error:
                logger.warning(f"Transformation error on row {i} for '{aspect_name}': {e}. Skipping.")
                continue
            raise TransformationError(f"Failed to transform {aspect_name} (row {i})") from e