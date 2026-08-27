from unittest.mock import MagicMock

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import NodePropertiesEndpoints
from graphdatascience.procedure_surface.api.catalog.node_property_endpoints import NodePropertyEndpoints


def test_stream_renames_property_column_to_property_value() -> None:
    delegate = MagicMock(spec=NodePropertiesEndpoints)
    delegate.stream.return_value = DataFrame({"nodeId": [0, 1], "pageRank": [0.15, 0.22]})

    result = NodePropertyEndpoints(delegate).stream(MagicMock(), "pageRank")

    assert list(result.columns) == ["nodeId", "propertyValue"]
    assert result["propertyValue"].tolist() == [0.15, 0.22]


def test_stream_keeps_node_labels_and_db_property_columns() -> None:
    delegate = MagicMock(spec=NodePropertiesEndpoints)
    delegate.stream.return_value = DataFrame(
        {
            "nodeId": [0],
            "nodeLabels": [["City"]],
            "population": [1000],
            "name": ["Malmö"],
        }
    )

    result = NodePropertyEndpoints(delegate).stream(MagicMock(), "population")

    assert list(result.columns) == ["nodeId", "nodeLabels", "propertyValue", "name"]


def test_stream_forwards_all_parameters_to_the_plural_endpoint() -> None:
    delegate = MagicMock(spec=NodePropertiesEndpoints)
    delegate.stream.return_value = DataFrame({"nodeId": [0], "population": [1000]})
    G = MagicMock()

    NodePropertyEndpoints(delegate).stream(
        G,
        "population",
        list_node_labels=True,
        node_labels=["City"],
        concurrency=4,
        sudo=True,
        log_progress=False,
        username="alice",
        db_node_properties=["name"],
    )

    delegate.stream.assert_called_once_with(
        G,
        "population",
        list_node_labels=True,
        node_labels=["City"],
        concurrency=4,
        sudo=True,
        log_progress=False,
        username="alice",
        db_node_properties=["name"],
    )
