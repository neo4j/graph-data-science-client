from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import CypherGraphBackend, get_graph
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


def _list_row(database: str = "dummy", **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "graphName": "g",
        "database": database,
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
        "degreeDistribution": {"mean": 1.75},
    }
    row.update(overrides)
    return row


def test_get_graph_builds_graph_with_cypher_backend() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION)
    graph = get_graph("g", runner)

    assert isinstance(graph, Graph)
    assert graph.name() == "g"


def test_graph_info_queries_graph_list() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.list": pd.DataFrame([_list_row()])})
    backend = CypherGraphBackend("g", runner)

    info = backend.graph_info()

    assert info.node_count == 4
    assert "gds.graph.list" in runner.queries[-1]
    assert runner.last_params()["graph_name"] == "g"


def test_graph_info_raises_when_graph_missing() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.list": pd.DataFrame()})
    backend = CypherGraphBackend("g", runner)

    with pytest.raises(ValueError, match="There is no projected graph named 'g'"):
        backend.graph_info()


def test_graph_info_filters_by_database_when_name_is_ambiguous() -> None:
    # Same graph name on two databases: backend must pick the row matching the runner's database ("dummy").
    rows = pd.DataFrame([_list_row(database="other", nodeCount=99), _list_row(database="dummy", nodeCount=4)])
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.list": rows})
    backend = CypherGraphBackend("g", runner)

    info = backend.graph_info()

    assert info.database == "dummy"
    assert info.node_count == 4


def test_exists_returns_flag() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.exists": pd.DataFrame([{"exists": True}])})
    backend = CypherGraphBackend("g", runner)

    assert backend.exists() is True
    assert "gds.graph.exists" in runner.queries[-1]
    assert runner.last_params()["graph_name"] == "g"


def test_exists_returns_native_bool_false() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.exists": pd.DataFrame([{"exists": False}])})
    backend = CypherGraphBackend("g", runner)

    result = backend.exists()

    # must be a native python bool, not a numpy bool
    assert result is False
    assert type(result) is bool


def test_drop_returns_graph_info() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.drop": pd.DataFrame([_list_row()])})
    backend = CypherGraphBackend("g", runner)

    result = backend.drop()

    assert result is not None
    assert result.graph_name == "g"
    assert runner.last_params()["failIfMissing"] is True


def test_drop_passes_fail_if_missing_flag() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.drop": pd.DataFrame([_list_row()])})
    backend = CypherGraphBackend("g", runner)

    backend.drop(fail_if_missing=False)

    assert runner.last_params()["failIfMissing"] is False


def test_drop_returns_none_when_result_empty() -> None:
    runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"gds.graph.drop": pd.DataFrame()})
    backend = CypherGraphBackend("g", runner)

    assert backend.drop(fail_if_missing=False) is None
