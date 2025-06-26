"""Step 2: Translate to Canonicalized Dictionaries (Streaming)"""
from typing import Any, Iterator
import msgpack
import logging
from tubuin_processor.schemas.aspects_raw import ASPECT_TO_RAW_SCHEMA_MAP
from tubuin_processor.core.exceptions import DecodingError, SchemaValidationError

logger = logging.getLogger(__name__)

def stream_decode_aspect(aspect_name: str, raw_bytes: bytes, skip_on_error: bool = False) -> Iterator[Any]:
    """Decodes a single aspect's raw bytes in a streaming fashion."""
    row_model_type = ASPECT_TO_RAW_SCHEMA_MAP.get(aspect_name)
    if not row_model_type:
        logger.warning(f"No raw Pydantic schema for aspect '{aspect_name}'. Skipping.")
        return

    unpacker = msgpack.Unpacker(raw=False, use_list=True)
    try:
        unpacker.feed(raw_bytes)
    except Exception as e:
        raise DecodingError(f"Failed to feed bytes to msgpack unpacker for {aspect_name}") from e

    i = 0
    for i, row_data_list in enumerate(unpacker):
        try:
            if not isinstance(row_data_list, list):
                raise SchemaValidationError(f"Row {i} is not a list.")
            yield row_model_type.from_list(row_data_list)
        except SchemaValidationError as e:
            if skip_on_error:
                logger.warning(f"Validation error on row {i} for '{aspect_name}': {e}. Skipping.")
                continue
            e.add_note(f"Error occurred on row {i} for aspect '{aspect_name}'")
            raise e
    logger.debug(f"Streamed and validated {i + 1} records for '{aspect_name}'.")