from typing import Iterator

import pytest
from pyarrow._flight import Result

from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


def test_deserialize_single_success() -> None:
    input_stream = iter([ArrowTestResult({"key": "value"})])
    expected = {"key": "value"}
    actual = deserialize_single(input_stream)
    assert expected == actual


def test_deserialize_single_raises_on_empty_stream() -> None:
    input_stream: Iterator[Result] = iter([])
    with pytest.raises(ValueError, match="Expected exactly one result, got 0"):
        deserialize_single(input_stream)


def test_deserialize_single_raises_on_multiple_results() -> None:
    input_stream = iter([ArrowTestResult({"key1": "value1"}), ArrowTestResult({"key2": "value2"})])
    with pytest.raises(ValueError, match="Expected exactly one result, got 2"):
        deserialize_single(input_stream)
