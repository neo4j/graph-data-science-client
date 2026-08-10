from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from graphdatascience.procedure_surface.cypher.catalog.projection_cypher_endpoints import ProjectCypherEndpoints
from graphdatascience.query_runner.query_mode import QueryMode
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


def _aggregation_row() -> dict[str, object]:
    return {
        "graphName": "offices",
        "nodeCount": 3,
        "relationshipCount": 4,
        "projectMillis": 42,
        "configuration": {"readConcurrency": 4},
        "query": "RETURN gds.graph.project(...)",
    }


def _drop_row() -> dict[str, object]:
    return {
        "graphName": "g",
        "database": "dummy",
        "databaseLocation": "local",
        "configuration": {},
        "memoryUsage": "1 KiB",
        "sizeInBytes": 1024,
        "nodeCount": 3,
        "relationshipCount": 4,
        "creationTime": datetime(2024, 1, 1),
        "modificationTime": datetime(2024, 1, 2),
        "schemaWithOrientation": {"nodes": {"Node": {}}, "relationships": {"REL": {}}},
        "density": 0.25,
    }


def _project_row() -> dict[str, object]:
    return {
        "graphName": "g",
        "nodeCount": 3,
        "relationshipCount": 4,
        "projectMillis": 42,
        "nodeProjection": {"Node": {}},
        "relationshipProjection": {"REL": {}},
    }


def _project_endpoints() -> tuple[ProjectCypherEndpoints, CollectingQueryRunner]:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "gds.graph.drop": pd.DataFrame([_drop_row()]),
            "gds.graph.project": pd.DataFrame([_project_row()]),
        },
    )
    return ProjectCypherEndpoints(runner), runner


def test_cypher_projection_returns_graph_and_typed_result() -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"gds.graph.project": pd.DataFrame({"g": [_aggregation_row()]})}
    )
    endpoints = ProjectCypherEndpoints(runner)

    G, result = endpoints.cypher(
        "MATCH (n)-->(m) RETURN gds.graph.project($graph_name, n, m, {})",
        database="neo4j",
        graph_name="offices",
        label="City",
    )

    assert G.name() == "offices"
    assert result.graph_name == "offices"
    assert result.node_count == 3
    assert result.relationship_count == 4
    assert result.project_millis == 42
    # Only the query parameters are forwarded; `database` is a dedicated argument.
    assert runner.last_params() == {"graph_name": "offices", "label": "City"}
    assert runner.last_run_args()["mode"] == QueryMode.READ


def test_cypher_projection_rejects_non_aggregation_result() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"RETURN 1": pd.DataFrame({"a": [1], "b": [2]})})
    endpoints = ProjectCypherEndpoints(runner)

    with pytest.raises(ValueError, match="must end with a single"):
        endpoints.cypher("RETURN 1 AS a, 2 AS b")


def test_native_overwrite_drops_existing_graph_first() -> None:
    endpoints, runner = _project_endpoints()

    endpoints.native("g", "Node", "REL", overwrite=True)

    assert "gds.graph.drop" in runner.queries[0]
    assert "gds.graph.project" in runner.queries[1]

    drop_params = runner.params[0]
    assert drop_params["graphName"] == "g"
    assert drop_params["failIfMissing"] is False
    assert runner.run_args[0]["retryable"] is True
    assert runner.run_args[0]["mode"] == QueryMode.WRITE


def test_native_does_not_drop_by_default() -> None:
    endpoints, runner = _project_endpoints()

    endpoints.native("g", "Node", "REL")

    assert not any("gds.graph.drop" in q for q in runner.queries)
    assert any("gds.graph.project" in q for q in runner.queries)


def test_native_overwrite_drops_as_impersonated_user() -> None:
    endpoints, runner = _project_endpoints()

    endpoints.native("g", "Node", "REL", overwrite=True, username="alice")

    drop_params = runner.params[0]
    assert drop_params["graphName"] == "g"
    assert drop_params["failIfMissing"] is False
    # the drop must run as the impersonated user so a graph owned by them is removed
    assert drop_params["username"] == "alice"
    assert drop_params["dbName"] == ""
