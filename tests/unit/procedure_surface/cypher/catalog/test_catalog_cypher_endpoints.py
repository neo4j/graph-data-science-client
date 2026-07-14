from __future__ import annotations

from datetime import datetime

import pandas as pd

from graphdatascience.procedure_surface.cypher.catalog.catalog_cypher_endpoints import CatalogCypherEndpoints
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
