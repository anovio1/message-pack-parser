# src\message_pack_parser\core\encoders\columnar_encoder.py

from typing import List, Any

import numpy as np
import polars as pl
import msgpack
import logging

from message_pack_parser.core.exceptions import OutputGenerationError

logger = logging.getLogger(__name__)


_NUMPY_DTYPE_MAP = {
    pl.Int8: "int8",
    pl.UInt8: "uint8",
    pl.Int16: "int16",
    pl.UInt16: "uint16",
    pl.Int32: "int32",
    pl.UInt32: "uint32",
    pl.Int64: "int64",
    pl.UInt64: "uint64",
    pl.Float32: "float32",
    pl.Float64: "float64",
    pl.Boolean: "uint8",
}


def _series_to_bytes_recursive(
    series: pl.Series, base_name: str
) -> tuple[dict[str, bytes], List[dict[str, Any]]]:  # Return type
    dtype = series.dtype

    # ... (List(Struct), List<primitive>, Utf8/String blocks remain the same) ...
    if isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.Struct):
        fields = dtype.inner.fields
        list_offs = [0]
        struct_count = 0
        flat_vals: dict[str, List[Any]] = {f.name: [] for f in fields}
        for list_item in series:
            if list_item is not None:
                for struct in list_item:
                    for f_def in fields:
                        flat_vals[f_def.name].append(
                            struct.get(f_def.name) if struct else None
                        )
                    struct_count += 1
            list_offs.append(struct_count)
        blobs: dict[str, bytes] = {}
        struct_schemas: List[dict[str, Any]] = []
        blobs[f"{base_name}_list_offs"] = np.asarray(
            list_offs, dtype="uint32"
        ).tobytes()
        for f_def in fields:
            f_name = f_def.name
            f_series = pl.Series(
                name=f_name, values=flat_vals[f_name], dtype=f_def.dtype, strict=False
            )
            f_blobs, f_sch = _series_to_bytes_recursive(
                f_series, f"{base_name}__{f_name}"
            )
            blobs.update(f_blobs)
            struct_schemas.extend(f_sch)
        return blobs, [
            {
                "name": base_name,
                "dtype": str(dtype),
                "list_offsets_key": f"{base_name}_list_offs",
                "struct_fields": struct_schemas,
            }
        ]

    if isinstance(dtype, pl.List) and dtype.inner in _NUMPY_DTYPE_MAP:
        inner_np = _NUMPY_DTYPE_MAP[dtype.inner]
        offs = [0]
        parts: List[bytes] = []
        count = 0  # Ensure parts is list of bytes
        is_flt = dtype.inner in (pl.Float32, pl.Float64)
        for sub_list in series:
            if sub_list is None:
                parts.append(b"")
            else:
                proc = []
                for item in sub_list:
                    if item is None:
                        if is_flt:
                            proc.append(np.nan)
                        else:
                            raise ValueError(
                                f"'{base_name}': list has None in non-float inner {dtype.inner}."
                            )
                    else:
                        proc.append(item)
                if proc:
                    arr = np.asarray(proc, dtype=inner_np)
                    parts.append(arr.tobytes())
                    count += len(arr)
                else:
                    parts.append(b"")
            offs.append(count)
        blobs_dict: dict[str, bytes] = {  # Explicit type
            f"{base_name}_offs": np.asarray(offs, dtype="uint32").tobytes(),
            f"{base_name}_data": b"".join(parts),
        }
        schema_list: List[dict[str, Any]] = [  # Explicit type
            {
                "name": base_name,
                "dtype": str(dtype),
                "data_key": f"{base_name}_data",
                "offsets_key": f"{base_name}_offs",
            }
        ]
        return blobs_dict, schema_list

    if dtype == pl.Utf8 or dtype == pl.String:
        enc_parts: List[bytes] = [
            s.encode("utf-8", "surrogatepass") if s is not None else b"" for s in series
        ]  # Ensure list of bytes
        offs = [0]
        l = 0
        for p in enc_parts:
            l += len(p)
            offs.append(l)
        blobs_dict: dict[str, bytes] = {  # Explicit type
            f"{base_name}_offs": np.asarray(offs, dtype="uint32").tobytes(),
            f"{base_name}_data": b"".join(enc_parts),
        }
        schema_list: List[dict[str, Any]] = [  # Explicit type
            {
                "name": base_name,
                "dtype": "Utf8",
                "data_key": f"{base_name}_data",
                "offsets_key": f"{base_name}_offs",
            }
        ]
        return blobs_dict, schema_list

    expected_np_dtype_str = _NUMPY_DTYPE_MAP.get(dtype)
    if expected_np_dtype_str:
        if series.has_nulls() and dtype not in (pl.Float32, pl.Float64):
            raise ValueError(
                f"Series '{base_name}' ({dtype}) has nulls. Fill for non-float primitives."
            )

        np_arr = series.to_numpy()
        actual_np_dtype_name = np_arr.dtype.name

        if actual_np_dtype_name != expected_np_dtype_str:
            logger.info(
                f"Series '{base_name}' (Polars: {dtype}, NumPy initial: {actual_np_dtype_name}): Casting to target NumPy '{expected_np_dtype_str}'."
            )
            try:
                np_arr = np_arr.astype(expected_np_dtype_str, copy=False)
            except Exception as e:
                raise OutputGenerationError(
                    f"Cast fail for '{base_name}': {actual_np_dtype_name} to {expected_np_dtype_str}. Polars: {dtype}. Err: {e}"
                ) from e
            if np_arr.dtype.name != expected_np_dtype_str:
                raise OutputGenerationError(
                    f"Post-cast dtype error for '{base_name}': tried '{expected_np_dtype_str}', got '{np_arr.dtype.name}'."
                )

        blobs_dict: dict[str, bytes] = {
            f"{base_name}_bin": np_arr.tobytes()
        }  # Explicit type
        schema_list: List[dict[str, Any]] = [
            {"name": base_name, "dtype": str(dtype), "data_key": f"{base_name}_bin"}
        ]  # Explicit type
        return blobs_dict, schema_list

    logger.warning(f"Series '{base_name}' ({dtype}) falling to MsgPack.")
    try:
        # Ensure packed_value is typed as bytes immediately.
        # msgpack.packb is annotated to return bytes, but we can be explicit.
        packed_value: bytes = msgpack.packb(  # type: ignore[assignment]
            series.to_list(), use_bin_type=True
        )

        # The dictionary being returned must conform to dict[str, bytes].
        # This construction ensures it.
        blobs_to_return: dict[str, bytes] = {f"{base_name}_mpk": packed_value}

        schema_to_return: List[dict[str, Any]] = [
            {
                "name": base_name,
                "dtype": str(dtype),
                "data_key": f"{base_name}_mpk",
                "serialization_method": "msgpack_polars_list",
            }
        ]
        return blobs_to_return, schema_to_return

    except Exception as e:
        logger.error(f"MsgPack fail '{base_name}' ({dtype}): {e}")
        # This path raises, so it doesn't return a problematic type.
        raise TypeError(
            f"Unhandled dtype {dtype} for '{base_name}' and MsgPack fallback failed."
        ) from e


def _series_to_bytes(
    series: pl.Series,
) -> tuple[dict[str, bytes], List[dict[str, Any]]]:
    return _series_to_bytes_recursive(series, series.name)

def _fill_nulls_per_contract(
    series: pl.Series,
    col_meta: dict[str, Any],
    table_meta: dict[str, Any],
    stream_name: str
) -> pl.Series:
    """
    Replace nulls according to the column’s `null_encoding`
    or the table-level default.  Raises if neither is supplied
    and the dtype is a non-float primitive.
    """
    if not series.has_nulls():
        return series                      # nothing to do

    sentinel = col_meta.get("null_encoding",
                            table_meta.get("null_encoding"))

    if sentinel is None and series.dtype not in (pl.Float32, pl.Float64):
        raise OutputGenerationError(
            f"{stream_name}.{series.name} has nulls but no null_encoding rule."
        )

    return series.fill_null(sentinel) if sentinel is not None else series
