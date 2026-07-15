from __future__ import annotations

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
