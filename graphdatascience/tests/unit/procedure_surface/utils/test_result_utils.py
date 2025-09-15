from typing import Generator

import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame

from graphdatascience import QueryRunner, ServerVersion
from graphdatascience.procedure_surface.utils.result_utils import transpose_property_columns, join_db_node_properties
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


@pytest.fixture
def mock_query_runner() -> Generator[CollectingQueryRunner, None, None]:
    yield CollectingQueryRunner(ServerVersion.from_string("1.2.3"), {
        "n.`property1` AS `property1`, n.`property2` AS `property2`": pd.DataFrame({
            "nodeId": [1, 2],
            "property1": ["value1", "value2"],
            "property2": ["valueA", "valueB"]
        })
    })

def test_transpose_property_columns_basic() -> None:
    data = {
        "nodeId": [1, 1, 2, 2],
        "nodeProperty": ["propA", "propB", "propA", "propB"],
        "propertyValue": [10, 20, 30, 40],
    }
    result = DataFrame(data)

    transposed_result = transpose_property_columns(result, list_node_labels=False)

    expected_data = {
        "nodeId": [1, 2],
        "propA": [10, 30],
        "propB": [20, 40],
    }
    expected_result = DataFrame(expected_data)

    assert expected_result.equals(transposed_result)


def test_transpose_property_columns_with_labels() -> None:
    data = {
        "nodeId": [1, 1, 2, 2],
        "nodeProperty": ["propA", "propB", "propA", "propB"],
        "propertyValue": [10, 20, 30, 40],
        "nodeLabels": [["Label1"], ["Label1"], ["Label2"], ["Label2"]],
    }
    result = DataFrame(data)

    transposed_result = transpose_property_columns(result, list_node_labels=True)

    expected_data = {
        "nodeId": [1, 2],
        "propA": [10, 30],
        "propB": [20, 40],
        "nodeLabels": [["Label1"], ["Label2"]],
    }
    expected_result = DataFrame(expected_data)

    assert expected_result.equals(transposed_result)


def test_transpose_property_columns_empty() -> None:
    result = DataFrame(columns=["nodeId", "nodeProperty", "propertyValue"])

    transposed_result = transpose_property_columns(result, list_node_labels=False)

    assert transposed_result.empty

def test_join_db_node_properties_basic(mock_query_runner: QueryRunner):
    input = pd.DataFrame({"nodeId": [1, 2]})
    db_node_properties = ["property1", "property2"]

    output = join_db_node_properties(input, db_node_properties, mock_query_runner)

    expected_output = pd.DataFrame({
        "nodeId": [1, 2],
        "property1": ["value1", "value2"],
        "property2": ["valueA", "valueB"]
    })

    pd.testing.assert_frame_equal(expected_output, output)


def test_join_db_node_properties_empty_input(mock_query_runner: QueryRunner):
    input = pd.DataFrame({"nodeId": []})
    db_node_properties = ["property1", "property2"]

    output = join_db_node_properties(input, db_node_properties, mock_query_runner)

    assert output.columns.tolist() == ["nodeId", "property1", "property2"]
    assert output.empty

def test_join_db_node_properties_non_matching_ids(mock_query_runner: QueryRunner):
    input = pd.DataFrame({"nodeId": [3, 4]})
    db_node_properties = ["property1", "property2"]

    output = join_db_node_properties(input, db_node_properties, mock_query_runner)

    assert set(output.columns) == {"nodeId", "property1", "property2"}
    assert output["nodeId"].tolist() == [3, 4]
    assert output["property1"].isna().all()
    assert output["property2"].isna().all()


def test_join_db_node_properties_partial_match(mock_query_runner: QueryRunner):
    input = pd.DataFrame({"nodeId": [1, 3]})
    db_node_properties = ["property1", "property2"]

    output = join_db_node_properties(input, db_node_properties, mock_query_runner)

    expected_output = pd.DataFrame({
        "nodeId": [1, 3],
        "property1": ["value1", np.nan],
        "property2": ["valueA", np.nan]
    })

    pd.testing.assert_frame_equal(expected_output, output)
