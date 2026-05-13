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
