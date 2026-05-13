from unittest import mock

import pandas as pd

from graphdatascience.procedure_surface.cypher.catalog.catalog_cypher_endpoints import CatalogCypherEndpoints


def _graph_info_row() -> dict[str, object]:
    return {
        "graphName": "g",
        "database": "neo4j",
        "databaseLocation": "local",
        "configuration": {},
        "memoryUsage": "1 KiB",
        "sizeInBytes": 42,
        "nodeCount": 1,
        "relationshipCount": 0,
        "creationTime": "2024-01-01T00:00:00Z",
        "modificationTime": "2024-01-01T00:00:00Z",
        "schemaWithOrientation": {"nodes": {}, "relationships": {}},
        "density": 0.0,
    }


def test_drop_passes_username_and_db_name_for_graph_name() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame([_graph_info_row()])

    result = CatalogCypherEndpoints(query_runner).drop("g", username="alice", db_name="analytics")

    assert result is not None
    query_runner.call_procedure.assert_called_once_with(
        endpoint="gds.graph.drop",
        params=mock.ANY,
    )
    assert query_runner.call_procedure.call_args.kwargs["params"] == {
        "graphName": "g",
        "failIfMissing": True,
        "dbName": "analytics",
        "username": "alice",
    }


def test_drop_passes_username_and_db_name_for_graph_object() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame([_graph_info_row()])
    graph = mock.Mock()
    graph.name.return_value = "g"

    CatalogCypherEndpoints(query_runner).drop(graph, fail_if_missing=False, username="alice", db_name="analytics")

    assert query_runner.call_procedure.call_args.kwargs["params"] == {
        "graphName": "g",
        "failIfMissing": False,
        "dbName": "analytics",
        "username": "alice",
    }


def test_filter_passes_parameters_sudo_and_username() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame(
        [
            {
                "graphName": "filtered",
                "fromGraphName": "g",
                "nodeFilter": "n.id >= $min_id",
                "relationshipFilter": "*",
                "nodeCount": 2,
                "relationshipCount": 1,
                "projectMillis": 7,
            }
        ]
    )
    graph = mock.Mock()
    graph.name.return_value = "g"

    CatalogCypherEndpoints(query_runner).filter(
        graph,
        graph_name="filtered",
        node_filter="n.id >= $min_id",
        relationship_filter="*",
        parameters={"min_id": 1},
        sudo=True,
        username="alice",
    )

    query_runner.call_procedure.assert_called_once_with(
        endpoint="gds.graph.filter",
        params=mock.ANY,
        logging=True,
    )
    params = query_runner.call_procedure.call_args.kwargs["params"]
    assert params["graph_name"] == "filtered"
    assert params["from_graph_name"] == "g"
    assert params["node_filter"] == "n.id >= $min_id"
    assert params["relationship_filter"] == "*"
    assert params["config"]["parameters"] == {"min_id": 1}
    assert params["config"]["sudo"] is True
    assert params["config"]["username"] == "alice"
    assert "jobId" in params["config"]
