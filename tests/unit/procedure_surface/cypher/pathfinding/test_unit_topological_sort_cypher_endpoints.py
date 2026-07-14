import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.pathfinding.topological_sort_cypher_endpoints import (
    TopologicalSortCypherEndpoints,
)
from tests.unit.conftest import CollectingQueryRunner


@pytest.fixture
def topological_sort_endpoints(query_runner: CollectingQueryRunner) -> TopologicalSortCypherEndpoints:
    return TopologicalSortCypherEndpoints(query_runner)


def test_stream_basic(
    topological_sort_endpoints: TopologicalSortCypherEndpoints,
    graph: Graph,
    query_runner: CollectingQueryRunner,
) -> None:
    topological_sort_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.dag.topologicalSort.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["computeMaxDistanceFromSource"] is False
    assert "jobId" in config


def test_stream_with_max_distance(
    topological_sort_endpoints: TopologicalSortCypherEndpoints,
    graph: Graph,
    query_runner: CollectingQueryRunner,
) -> None:
    topological_sort_endpoints.stream(graph, compute_max_distance_from_source=True)

    assert len(query_runner.queries) == 1
    assert "gds.dag.topologicalSort.stream" in query_runner.queries[0]
    config = query_runner.params[0]["config"]
    assert config["computeMaxDistanceFromSource"] is True
