import dataclasses
import json
from dataclasses import fields
from typing import Any, Dict, Iterator, Type, TypeVar

from pyarrow._flight import Result


class DataMapper:
    T = TypeVar("T")

    @staticmethod
    def deserialize_single(input_stream: Iterator[Result], cls: Type[T]) -> T:
        rows = DataMapper.deserialize(input_stream, cls)

        if len(rows) != 1:
            raise ValueError(f"Expected exactly one row, got {len(rows)}")

        return rows[0]

    @staticmethod
    def deserialize(input_stream, cls: Type[T]) -> list[T]:
        def deserialize_row(row: Any):
            result_dicts = json.loads(row.body.to_pybytes().decode())
            if cls == Dict:
                return result_dicts
            return DataMapper.dict_to_dataclass(result_dicts, cls)

        return [deserialize_row(row) for row in list(input_stream)]

    @staticmethod
    def dict_to_dataclass(data: Dict[str, Any], cls: Type[T], strict: bool = False) -> T:
        """
        Convert a dictionary to a dataclass instance with nested dataclass support.
        """
        if not dataclasses.is_dataclass(cls):
            raise ValueError(f"{cls} is not a dataclass")

        field_dict = {f.name: f for f in fields(cls)}
        filtered_data = {}

        for key, value in data.items():
            if key in field_dict:
                field = field_dict[key]
                field_type = field.type

                # Handle nested dataclasses
                if dataclasses.is_dataclass(field_type) and isinstance(value, dict):
                    filtered_data[key] = DataMapper.dict_to_dataclass(value, field_type, strict)
                else:
                    filtered_data[key] = value
            elif strict:
                raise ValueError(f"Extra field '{key}' not allowed in {cls.__name__}")

        return cls(**filtered_data)
