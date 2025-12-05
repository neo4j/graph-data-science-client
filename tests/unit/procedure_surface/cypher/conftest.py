from typing import Any

import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


@pytest.fixture
def query_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(DEFAULT_SERVER_VERSION)


@pytest.fixture
def graph(query_runner: CollectingQueryRunner) -> GraphV2:
    return get_graph("test_graph", query_runner)


def estimate_mock_result() -> dict[str, Any]:
    return {
        "nodeCount": 100,
        "relationshipCount": 200,
        "requiredMemory": "1024 Bytes",
        "bytesMin": 1024,
        "bytesMax": 2048,
        "heapPercentageMin": 1.0,
        "heapPercentageMax": 2.0,
        "treeView": "1024 KiB",
        "mapView": {},
    }
