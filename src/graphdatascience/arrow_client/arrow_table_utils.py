from __future__ import annotations

import pandas
import pyarrow
from pyarrow import types as pa_types


def _downcast_type(data_type: pyarrow.DataType) -> pyarrow.DataType:
    """Map pyarrow "large" string/binary types back to their 32-bit variants.

    pandas 3.0 introduced a default string dtype that ``pyarrow.Table.from_pandas``
    maps to ``large_string`` (LargeUTF8), whereas pandas 2.x produced ``string``
    (UTF8). The GDS server expects UTF8, so we coerce the large variants back.
    List containers are preserved as-is, only their element type is downcast.
    """
    if pa_types.is_large_string(data_type):
        return pyarrow.string()

    return data_type


def table_from_pandas(data: pandas.DataFrame) -> pyarrow.Table:
    """Convert a DataFrame to a pyarrow Table, coercing large string/binary types to UTF8/binary.

    Use this instead of ``pyarrow.Table.from_pandas`` so the produced schema stays
    consistent across pandas 2.x and 3.0 (see :func:`_downcast_type`).
    """
    table = pyarrow.Table.from_pandas(data)

    new_types = [_downcast_type(field.type) for field in table.schema]
    if all(new_type == field.type for new_type, field in zip(new_types, table.schema)):
        return table

    new_schema = pyarrow.schema(
        [field.with_type(new_type) for field, new_type in zip(table.schema, new_types)],
        metadata=table.schema.metadata,
    )
    return table.cast(new_schema)
