from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph
from graphdatascience.procedure_surface.cypher.catalog.graph_sampling_cypher_endpoints import (
    GraphSamplingCypherEndpoints,
)
from graphdatascience.query_runner.query_mode import QueryMode
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


def _drop_row() -> dict[str, object]:
    return {
        "graphName": "sampled",
        "database": "dummy",
        "databaseLocation": "local",
        "configuration": {},
        "memoryUsage": "1 KiB",
        "sizeInBytes": 1024,
        "nodeCount": 2,
        "relationshipCount": 1,
        "creationTime": datetime(2024, 1, 1),
        "modificationTime": datetime(2024, 1, 2),
        "schemaWithOrientation": {"nodes": {"Node": {}}, "relationships": {"REL": {}}},
        "density": 0.25,
    }


def _sample_row() -> dict[str, object]:
    return {
        "graphName": "sampled",
        "fromGraphName": "g",
        "nodeCount": 2,
        "relationshipCount": 1,
        "startNodeCount": 1,
        "projectMillis": 7,
    }


def _endpoints() -> tuple[GraphSamplingCypherEndpoints, CollectingQueryRunner]:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "gds.graph.drop": pd.DataFrame([_drop_row()]),
            "gds.graph.sample.rwr": pd.DataFrame([_sample_row()]),
            "gds.graph.sample.cnarw": pd.DataFrame([_sample_row()]),
        },
    )
    return GraphSamplingCypherEndpoints(runner), runner


def test_rwr_overwrite_drops_existing_graph_first() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    sampled, result = endpoints.rwr(G, "sampled", overwrite=True)

    assert "gds.graph.drop" in runner.queries[0]
    assert "gds.graph.sample.rwr" in runner.queries[1]
    assert runner.params[0]["graph_name"] == "sampled"
    assert runner.params[0]["failIfMissing"] is False
    assert runner.run_args[0]["retryable"] is True
    assert runner.run_args[0]["mode"] == QueryMode.WRITE
    assert result.graph_name == "sampled"


def test_rwr_rejects_name_equal_to_source_graph() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    with pytest.raises(ValueError, match="must not equal the source graph name"):
        endpoints.rwr(G, "g", overwrite=True)

    assert not any("gds.graph.drop" in q for q in runner.queries)


def test_cnarw_rejects_name_equal_to_source_graph() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    with pytest.raises(ValueError, match="must not equal the source graph name"):
        endpoints.cnarw(G, "g", overwrite=True)

    assert not any("gds.graph.drop" in q for q in runner.queries)


def test_rwr_does_not_drop_by_default() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    endpoints.rwr(G, "sampled")

    assert not any("gds.graph.drop" in q for q in runner.queries)
    assert any("gds.graph.sample.rwr" in q for q in runner.queries)


def test_cnarw_overwrite_drops_existing_graph_first() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    sampled, result = endpoints.cnarw(G, "sampled", overwrite=True)

    assert "gds.graph.drop" in runner.queries[0]
    assert "gds.graph.sample.cnarw" in runner.queries[1]
    assert runner.params[0]["graph_name"] == "sampled"
    assert runner.params[0]["failIfMissing"] is False
    assert result.graph_name == "sampled"


def test_cnarw_does_not_drop_by_default() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    endpoints.cnarw(G, "sampled")

    assert not any("gds.graph.drop" in q for q in runner.queries)
    assert any("gds.graph.sample.cnarw" in q for q in runner.queries)


def test_rwr_overwrite_drops_as_impersonated_user() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    endpoints.rwr(G, "sampled", overwrite=True, username="alice")

    drop_params = runner.params[0]
    assert drop_params["graph_name"] == "sampled"
    assert drop_params["failIfMissing"] is False
    assert drop_params["username"] == "alice"
    assert drop_params["dbName"] == ""


def test_cnarw_overwrite_drops_as_impersonated_user() -> None:
    endpoints, runner = _endpoints()
    G = get_graph("g", runner)

    endpoints.cnarw(G, "sampled", overwrite=True, username="alice")

    drop_params = runner.params[0]
    assert drop_params["graph_name"] == "sampled"
    assert drop_params["failIfMissing"] is False
    assert drop_params["username"] == "alice"
    assert drop_params["dbName"] == ""
