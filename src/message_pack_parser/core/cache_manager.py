"""Step 3: Cache Qualified Data"""
from typing import Dict, List, Optional
import os
import msgpack
import hashlib
import logging
from message_pack_parser.schemas.aspects_raw import BaseAspectDataPointRaw, ASPECT_TO_RAW_SCHEMA_MAP
from message_pack_parser.core.exceptions import CacheReadError, CacheWriteError, CacheValidationError, SchemaValidationError

logger = logging.getLogger(__name__)

def _get_pipeline_version_hash() -> str:
    """Generates a hash based on critical pipeline modules to version the cache."""
    hasher = hashlib.md5()
    files_to_hash = [
        'src/message_pack_parser/schemas/aspects_raw.py', 'src/message_pack_parser/schemas/aspects.py',
        'src/message_pack_parser/config/enums.py', 'src/message_pack_parser/config/dynamic_config_builder.py',
        'src/message_pack_parser/core/decoder.py', 'src/message_pack_parser/core/value_transformer.py'
    ]
    for filepath in files_to_hash:
        try:
            with open(filepath, 'rb') as f:
                hasher.update(f.read())
        except FileNotFoundError:
            logger.warning(f"Could not find file {filepath} for cache versioning.")
            hasher.update(filepath.encode())
    return hasher.hexdigest()

def _get_cache_filepath(cache_dir: str, replay_id: str) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{replay_id}_qualified_data.mpkcache")

def save_to_cache(data_by_aspect: Dict[str, List[BaseAspectDataPointRaw]], cache_dir: str, replay_id: str):
    cache_filepath = _get_cache_filepath(cache_dir, replay_id)
    pipeline_version = _get_pipeline_version_hash()
    serializable_data = {
        name: [
            item.model_dump for item in aspect_list
            ] 
        for name, aspect_list in data_by_aspect.items()
    }
    cache_payload = {'version': pipeline_version, 'data': serializable_data}
    try:
        with open(cache_filepath, "wb") as f:
            packed_data = msgpack.packb(cache_payload, use_bin_type=True)
            assert(isinstance(packed_data, bytes))
            f.write(packed_data)
        logger.info(f"Data cached successfully with version: {pipeline_version[:7]}")
    except (IOError, TypeError) as e:
        raise CacheWriteError(f"Failed to write to cache file {cache_filepath}") from e

def load_from_cache(cache_dir: str, replay_id: str) -> Optional[Dict[str, List[BaseAspectDataPointRaw]]]:
    cache_filepath = _get_cache_filepath(cache_dir, replay_id)
    if not os.path.exists(cache_filepath): return None

    current_pipeline_version = _get_pipeline_version_hash()
    try:
        with open(cache_filepath, "rb") as f: cache_payload = msgpack.unpackb(f.read(), raw=False)
    except (msgpack.UnpackException, IOError) as e:
        raise CacheReadError(f"Failed to read or unpack cache file {cache_filepath}") from e

    cached_version = cache_payload.get('version')
    if cached_version != current_pipeline_version:
        raise CacheValidationError(f"Cache version mismatch. Expected '{current_pipeline_version[:7]}', found '{str(cached_version)[:7]}'. Cache is stale.")

    raw_data_dict = cache_payload.get('data', {})
    reconstructed_data: Dict[str, List[BaseAspectDataPointRaw]] = {}
    for name, list_of_dicts in raw_data_dict.items():
        if name in ASPECT_TO_RAW_SCHEMA_MAP:
            row_model_type = ASPECT_TO_RAW_SCHEMA_MAP[name]
            try:
                reconstructed_data[name] = [row_model_type.model_validate(d) for d in list_of_dicts]
            except SchemaValidationError as e:
                raise CacheValidationError(f"Failed to re-validate cached data for {name}") from e
        else:
            logger.warning(f"No raw schema for cached aspect '{name}'. Skipping.")
    logger.info(f"Successfully loaded and validated data from cache with version: {cached_version[:7]}")
    return reconstructed_data