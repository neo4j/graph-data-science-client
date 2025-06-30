from dataclasses import dataclass
from typing import Any, Dict

import pytest

from graphdatascience.arrow_client.data_mapper import DataMapper


@dataclass
class NestedDataclass:
    nested_field: int


@dataclass
class ExampleDataclass:
    field_one: str
    field_two: int
    nested: NestedDataclass


@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        (
            {"field_one": "test", "field_two": 123, "nested": {"nested_field": 456}},
            ExampleDataclass("test", 123, NestedDataclass(456)),
        ),
    ],
)
def test_dict_to_dataclass(input_data: Dict[str, Any], expected_output: ExampleDataclass) -> None:
    result = DataMapper.dict_to_dataclass(input_data, ExampleDataclass)
    assert result == expected_output


def test_dict_to_dataclass_strict_mode_rejects_extra_fields() -> None:
    input_data = {"field_one": "test", "field_two": 123, "nested": {"nested_field": 456}, "extra_field": "not_allowed"}

    with pytest.raises(ValueError, match="Extra field 'extra_field' not allowed in ExampleDataclass"):
        DataMapper.dict_to_dataclass(input_data, ExampleDataclass, strict=True)


def test_dict_to_dataclass_non_dataclass_error() -> None:
    with pytest.raises(ValueError, match="is not a dataclass"):
        DataMapper.dict_to_dataclass({"key": "value"}, int)
