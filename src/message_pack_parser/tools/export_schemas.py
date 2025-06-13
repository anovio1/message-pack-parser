"""
A command-line tool for exporting Pydantic raw schemas to a machine-readable
JSON format.

This tool introspects the Pydantic models defined in aspects_raw.py to provide
a complete definition of the expected raw data structure and transformation rules,
correctly reading from the `json_schema_extra` attribute.
"""
import json
import logging
from pathlib import Path
from typing import Any, List, Optional, Type, get_type_hints, get_args, get_origin, Dict
from enum import Enum

import typer
from pydantic import BaseModel

# Import the map of all registered raw schemas
from message_pack_parser.schemas.aspects_raw import ASPECT_TO_RAW_SCHEMA_MAP

# Setup
app = typer.Typer(
    help="A tool to export Pydantic data schemas to JSON format.",
    add_completion=False
)
logger = logging.getLogger(__name__)


def _format_type_hint(type_hint: Any) -> str:
    """Creates a clean string representation of a type hint."""
    origin = get_origin(type_hint)
    if origin is None:
        return getattr(type_hint, '__name__', str(type_hint))
    args = get_args(type_hint)
    arg_names = [getattr(arg, '__name__', str(arg)) for arg in args]
    return f"{origin.__name__}[{', '.join(arg_names)}]"


def _format_enum_map(enum_map_tuple: tuple) -> Optional[tuple]:
    """Converts the Enum class in an enum_map to its string name for JSON serialization."""
    if not enum_map_tuple or len(enum_map_tuple) != 2:
        return None
    
    clean_field_name, enum_class = enum_map_tuple
    if isinstance(enum_class, type) and issubclass(enum_class, Enum):
        return (clean_field_name, enum_class.__name__)
    return None


def export_pydantic_schema(model_cls: Type[BaseModel]) -> Dict[str, Any]:
    """
    Introspects a Pydantic model and exports its structure, types, and metadata
    from `json_schema_extra`.
    """
    logger.info(f"Exporting schema for model: {model_cls.__name__}")
    fields = []
    type_hints = get_type_hints(model_cls)

    for name, field_info in model_cls.model_fields.items():
        python_type = type_hints.get(name)

        # --- CORRECTED LOGIC TO READ FROM json_schema_extra ---
        extra_schema_data = field_info.json_schema_extra
        extra_schema = {} # Default to an empty dict

        if extra_schema_data:
            # If it's a callable function (like a lambda), execute it to get the dict.
            if callable(extra_schema_data):
                extra_schema = extra_schema_data({})
            # Ensure the result is a dict, otherwise keep it empty.
            elif isinstance(extra_schema_data, dict):
                extra_schema = extra_schema_data
        
        # Now safely get the metadata from the processed extra_schema dictionary
        dequant = extra_schema.get("dequantize_by")
        enum_map = _format_enum_map(extra_schema.get("enum_map"))
        # --- END OF CORRECTION ---

        field_data = {
            "name": name,
            "type": _format_type_hint(python_type),
            "dequantize_by": dequant,
            "enum_map": enum_map,
        }
        fields.append(field_data)
        
    source_aspect = None
    for aspect, schema_class in ASPECT_TO_RAW_SCHEMA_MAP.items():
        if schema_class == model_cls:
            source_aspect = aspect
            break

    return {
        "model_name": model_cls.__name__,
        "source_aspect": source_aspect,
        "fields": fields
    }

def save_schema_to_file(schema_dict: Dict, output_dir: Path):
    """Saves a schema dictionary to a JSON file."""
    # Use the source_aspect name for the file if available, otherwise the model name
    file_name_base = schema_dict["model_name"] or schema_dict["source_aspect"]
    output_path = output_dir / f"{file_name_base}_schema.json"
    
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(schema_dict, f, indent=2)
        logger.info(f"Successfully saved schema to {output_path}")
    except IOError as e:
        logger.error(f"Failed to write schema file to {output_path}: {e}")

@app.command()
def export(
    aspect_name: Optional[List[str]] = typer.Argument(None, help="The name(s) of the aspect schema(s) to export (e.g., 'team_stats')."),
    all_schemas: bool = typer.Option(False, "--all", "-a", help="Export all registered raw schemas."),
    output_dir: Path = typer.Option("schema_exports/", "--output-dir", "-o", help="Directory to save the exported JSON files.")
):
    """
    Exports Pydantic schemas to JSON files for external use or documentation.
    """
    if not all_schemas and not aspect_name:
        logger.error("Error: You must specify an aspect name or use the --all flag.")
        raise typer.Exit(code=1)

    schemas_to_export: List[Type[BaseModel]] = []
    if all_schemas:
        # Use .values() to get the Pydantic classes directly
        schemas_to_export = list(ASPECT_TO_RAW_SCHEMA_MAP.values())
    else:
        for name in aspect_name:
            model_cls = ASPECT_TO_RAW_SCHEMA_MAP.get(name)
            if model_cls:
                schemas_to_export.append(model_cls)
            else:
                logger.warning(f"Schema for aspect '{name}' not found. Skipping.")
    
    if not schemas_to_export:
        logger.warning("No valid schemas found to export.")
        return

    logger.info(f"Found {len(schemas_to_export)} schemas to export to '{output_dir}'.")
    # Remove duplicates if --all is used and a name is also specified
    unique_schemas = list(dict.fromkeys(schemas_to_export))
    
    for model_cls in unique_schemas:
        schema_dict = export_pydantic_schema(model_cls)
        save_schema_to_file(schema_dict, output_dir)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    app()