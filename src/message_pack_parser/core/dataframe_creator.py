"""Step 5: Polars DataFrame Creation & Schema Application"""
from typing import Dict, List, Type, get_type_hints, get_origin, get_args
import polars as pl
from pydantic import BaseModel
from enum import Enum
import logging
from message_pack_parser.schemas.aspects import ASPECT_TO_CLEAN_SCHEMA_MAP, PYDANTIC_TO_POLARS_TYPE_MAP
from message_pack_parser.core.exceptions import ParserError

logger = logging.getLogger(__name__)

def _pydantic_to_polars_schema(pydantic_model: Type[BaseModel]) -> Dict[str, pl.DataType]:
    polars_schema = {}
    for name, type_hint in get_type_hints(pydantic_model).items():
        origin = get_origin(type_hint)
        if origin is list or origin is dict: continue # Skip complex types for now
        if origin is not None: # Handle Optional[T]
            non_none_args = [t for t in get_args(type_hint) if t is not type(None)]
            if len(non_none_args) == 1: type_hint = non_none_args[0]
        if issubclass(type_hint, Enum): polars_schema[name] = pl.Categorical
        else: polars_schema[name] = PYDANTIC_TO_POLARS_TYPE_MAP.get(type_hint, pl.Object)
    return polars_schema

def _model_to_dict_for_polars(model: BaseModel) -> Dict:
    d = model.model_dump()
    for k, v in d.items():
        if isinstance(v, Enum): d[k] = v.name
    return d

def create_polars_dataframe_for_aspect(aspect_name: str, clean_models: List[BaseModel]) -> pl.DataFrame:
    logger.debug(f"Creating DataFrame for aspect: {aspect_name}")
    clean_schema_type = ASPECT_TO_CLEAN_SCHEMA_MAP.get(aspect_name)
    if not clean_schema_type:
        raise ParserError(f"Cannot create DataFrame: No clean Pydantic schema for '{aspect_name}'.")

    try: polars_schema = _pydantic_to_polars_schema(clean_schema_type)
    except Exception as e: raise ParserError(f"Failed to derive Polars schema for {aspect_name}") from e

    if not clean_models:
        logger.warning(f"No data for '{aspect_name}'. Creating empty DataFrame.")
        return pl.DataFrame(schema=polars_schema)

    try:
        list_of_dicts = [_model_to_dict_for_polars(model) for model in clean_models]
        df = pl.DataFrame(data=list_of_dicts, schema=polars_schema)
        logger.debug(f"Created DataFrame for '{aspect_name}' with shape {df.shape}.")
        return df
    except Exception as e:
        raise ParserError(f"Failed to create Polars DataFrame for {aspect_name}") from e