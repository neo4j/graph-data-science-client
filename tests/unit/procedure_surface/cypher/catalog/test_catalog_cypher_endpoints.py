from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.catalog.catalog_cypher_endpoints import CatalogCypherEndpoints
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph
from graphdatascience.query_runner.query_mode import QueryMode
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


def _drop_row() -> dict[str, object]:
    return {
        "graphName": "g",
        "database": "dummy",
        "databaseLocation": "local",
        "configuration": {"jobId": "job-1"},
        "memoryUsage": "1 KiB",
        "sizeInBytes": 1024,
        "nodeCount": 4,
        "relationshipCount": 5,
        "creationTime": datetime(2024, 1, 1),
        "modificationTime": datetime(2024, 1, 2),
        "schemaWithOrientation": {"nodes": {"Node": {}}, "relationships": {"REL": {}}},
        "density": 0.25,
    }


def _generate_row() -> dict[str, object]:
    return {
        "name": "g",
        "nodes": 4,
        "relationships": 5,
        "generateMillis": 42,
        "relationshipSeed": 123,
        "averageDegree": 2.5,
        "relationshipDistribution": "UNIFORM",
        "relationshipProperty": None,
    }


def _filter_row() -> dict[str, object]:
    return {
        "graphName": "filtered",
        "fromGraphName": "g",
        "nodeFilter": "true",
        "relationshipFilter": "true",
        "nodeCount": 2,
        "relationshipCount": 1,
        "projectMillis": 7,
    }


def _endpoints() -> tuple[CatalogCypherEndpoints, CollectingQueryRunner]:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.drop": pd.DataFrame([_drop_row()])})
    return CatalogCypherEndpoints(runner), runner


def test_drop_passes_graph_name_and_fail_if_missing() -> None:
    endpoints, runner = _endpoints()

    result = endpoints.drop("g")

    assert result is not None
    assert result.graph_name == "g"
    params = runner.last_params()
    assert params["graphName"] == "g"
    assert params["failIfMissing"] is True
    assert "dbName" not in params
    assert "username" not in params


def test_drop_passes_db_name_and_username() -> None:
    endpoints, runner = _endpoints()

    endpoints.drop("g", db_name="neo4j", username="alice")

    params = runner.last_params()
    assert params["dbName"] == "neo4j"
    assert params["username"] == "alice"


def test_drop_defaults_db_name_when_only_username_given() -> None:
    endpoints, runner = _endpoints()

    endpoints.drop("g", username="alice")

    params = runner.last_params()
    # dbName must be present (defaulted) to keep the positional arguments aligned.
    assert params["dbName"] == ""
    assert params["username"] == "alice"


def test_drop_defaults_username_when_only_db_name_given() -> None:
    endpoints, runner = _endpoints()

    endpoints.drop("g", db_name="neo4j")

    params = runner.last_params()
    assert params["dbName"] == "neo4j"
    assert params["username"] == ""


def test_drop_is_retryable_only_when_missing_graphs_are_tolerated() -> None:
    endpoints, runner = _endpoints()

    endpoints.drop("g", fail_if_missing=False)
    assert runner.last_run_args()["retryable"] is True
    assert runner.last_run_args()["mode"] == QueryMode.WRITE

    endpoints.drop("g", fail_if_missing=True)
    assert runner.last_run_args()["retryable"] is False


def _construct_nodes() -> pd.DataFrame:
    return pd.DataFrame({"nodeId": [0, 1], "labels": [["A"], ["B"]]})


def test_construct_overwrite_drops_existing_graph_first() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "gds.graph.drop": pd.DataFrame([_drop_row()]),
            "gds.debug.sysInfo": pd.DataFrame([{"value": "Community"}]),
        },
    )
    endpoints = CatalogCypherEndpoints(runner)

    G = endpoints.construct(graph_name="g", nodes=_construct_nodes(), relationships=[], overwrite=True)

    assert "gds.graph.drop" in runner.queries[0]
    assert runner.params[0]["graph_name"] == "g"
    assert runner.params[0]["failIfMissing"] is False
    assert runner.run_args[0]["retryable"] is True
    assert isinstance(G, Graph)


def test_construct_does_not_drop_by_default() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {"gds.debug.sysInfo": pd.DataFrame([{"value": "Community"}])},
    )
    endpoints = CatalogCypherEndpoints(runner)

    endpoints.construct(graph_name="g", nodes=_construct_nodes(), relationships=[])

    assert not any("gds.graph.drop" in q for q in runner.queries)


def test_generate_overwrite_drops_existing_graph_first() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "gds.graph.drop": pd.DataFrame([_drop_row()]),
            "gds.graph.generate": pd.DataFrame([_generate_row()]),
        },
    )
    endpoints = CatalogCypherEndpoints(runner)

    G, result = endpoints.generate("g", 4, 2.5, overwrite=True)

    assert "gds.graph.drop" in runner.queries[0]
    assert "gds.graph.generate" in runner.queries[1]
    assert runner.params[0]["graph_name"] == "g"
    assert runner.params[0]["failIfMissing"] is False
    assert result.name == "g"


def test_generate_does_not_drop_by_default() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {"gds.graph.generate": pd.DataFrame([_generate_row()])},
    )
    endpoints = CatalogCypherEndpoints(runner)

    endpoints.generate("g", 4, 2.5)

    assert not any("gds.graph.drop" in q for q in runner.queries)
    assert any("gds.graph.generate" in q for q in runner.queries)


def test_filter_overwrite_drops_existing_graph_first() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "gds.graph.drop": pd.DataFrame([_drop_row()]),
            "gds.graph.filter": pd.DataFrame([_filter_row()]),
        },
    )
    endpoints = CatalogCypherEndpoints(runner)
    G = get_graph("g", runner)

    filtered, result = endpoints.filter(G, "filtered", "true", "true", overwrite=True)

    assert "gds.graph.drop" in runner.queries[0]
    assert "gds.graph.filter" in runner.queries[1]
    assert runner.params[0]["graph_name"] == "filtered"
    assert runner.params[0]["failIfMissing"] is False
    assert result.graph_name == "filtered"


def test_filter_does_not_drop_by_default() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {"gds.graph.filter": pd.DataFrame([_filter_row()])},
    )
    endpoints = CatalogCypherEndpoints(runner)
    G = get_graph("g", runner)

    endpoints.filter(G, "filtered", "true", "true")

    assert not any("gds.graph.drop" in q for q in runner.queries)
    assert any("gds.graph.filter" in q for q in runner.queries)


def test_filter_rejects_name_equal_to_source_graph() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {"gds.graph.filter": pd.DataFrame([_filter_row()])},
    )
    endpoints = CatalogCypherEndpoints(runner)
    G = get_graph("g", runner)

    with pytest.raises(ValueError, match="must not equal the source graph name"):
        endpoints.filter(G, "g", "true", "true", overwrite=True)

    assert not any("gds.graph.drop" in q for q in runner.queries)


def test_generate_overwrite_drops_as_impersonated_user() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "gds.graph.drop": pd.DataFrame([_drop_row()]),
            "gds.graph.generate": pd.DataFrame([_generate_row()]),
        },
    )
    endpoints = CatalogCypherEndpoints(runner)

    endpoints.generate("g", 4, 2.5, overwrite=True, username="alice")

    drop_params = runner.params[0]
    assert drop_params["graph_name"] == "g"
    assert drop_params["failIfMissing"] is False
    assert drop_params["username"] == "alice"
    assert drop_params["dbName"] == ""


def test_filter_overwrite_drops_as_impersonated_user() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "gds.graph.drop": pd.DataFrame([_drop_row()]),
            "gds.graph.filter": pd.DataFrame([_filter_row()]),
        },
    )
    endpoints = CatalogCypherEndpoints(runner)
    G = get_graph("g", runner)

    endpoints.filter(G, "filtered", "true", "true", overwrite=True, username="alice")

    drop_params = runner.params[0]
    assert drop_params["graph_name"] == "filtered"
    assert drop_params["failIfMissing"] is False
    assert drop_params["username"] == "alice"
    assert drop_params["dbName"] == ""
