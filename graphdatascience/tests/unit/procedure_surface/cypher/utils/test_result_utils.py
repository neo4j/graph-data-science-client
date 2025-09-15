from pandas import DataFrame

from graphdatascience.procedure_surface.utils.result_utils import transpose_property_columns


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
