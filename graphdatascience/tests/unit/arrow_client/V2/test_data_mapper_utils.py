import pytest

from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


def test_deserialize_single_success() -> None:
    expected = {"key": "value"}
    actual = deserialize_single([ArrowTestResult({"key": "value"})])
    assert expected == actual


def test_deserialize_single_raises_on_empty_stream() -> None:
    with pytest.raises(ValueError, match="Expected exactly one result, got 0"):
        deserialize_single([])


def test_deserialize_single_raises_on_multiple_results() -> None:
    with pytest.raises(ValueError, match="Expected exactly one result, got 2"):
        deserialize_single([ArrowTestResult({"key1": "value1"}), ArrowTestResult({"key2": "value2"})])
