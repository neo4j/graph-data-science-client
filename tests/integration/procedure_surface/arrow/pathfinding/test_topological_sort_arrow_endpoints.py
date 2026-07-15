from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.pathfinding.topological_sort_arrow_endpoints import (
    TopologicalSortArrowEndpoints,
)
from tests.integration.procedure_surface.arrow.graph_creation_helper import create_graph

TOPOLOGICAL_SORT_ACTION = "v2/pathfinding.topologicalSort"


@pytest.fixture(autouse=True)
def require_topological_sort(arrow_client: AuthenticatedArrowClient) -> None:
    # Topological sort over Arrow is on master but not in every released session image yet.
    # Skip against sessions that do not advertise the action so this suite is release-safe.
    advertised = {action.type for action in arrow_client.list_actions()}
    if TOPOLOGICAL_SORT_ACTION not in advertised:
        pytest.skip(f"Session does not advertise {TOPOLOGICAL_SORT_ACTION}")


# Create a DAG (Directed Acyclic Graph) for testing topological sort
dag_graph = """
        CREATE
            (a: Node {id: 0}),
            (b: Node {id: 1}),
            (c: Node {id: 2}),
            (d: Node {id: 3}),
            (e: Node {id: 4}),
            (f: Node {id: 5}),
            (a)-[:LINK]->(b),
            (a)-[:LINK]->(c),
            (b)-[:LINK]->(d),
            (c)-[:LINK]->(d),
            (d)-[:LINK]->(e),
            (c)-[:LINK]->(f),
            (f)-[:LINK]->(e)
        """


@pytest.fixture
def sample_dag(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "dag", dag_graph) as G:
        yield G


@pytest.fixture
def topological_sort_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[TopologicalSortArrowEndpoints, None, None]:
    yield TopologicalSortArrowEndpoints(arrow_client)


def test_topological_sort_stream(topological_sort_endpoints: TopologicalSortArrowEndpoints, sample_dag: Graph) -> None:
    result_df = topological_sort_endpoints.stream(G=sample_dag)

    assert len(result_df) == 6
    # The `index` column is consumed for ordering and dropped.
    assert "nodeId" in result_df.columns
    # Result is ordered topologically: the source node (0) comes first, the sink (4) last.
    assert result_df["nodeId"].iloc[0] == 0
    assert result_df["nodeId"].iloc[-1] == 4


def test_topological_sort_stream_with_max_distance(
    topological_sort_endpoints: TopologicalSortArrowEndpoints, sample_dag: Graph
) -> None:
    result_df = topological_sort_endpoints.stream(G=sample_dag, compute_max_distance_from_source=True)

    assert len(result_df) == 6
    assert {"nodeId", "maxDistanceFromSource"} == set(result_df.columns)
    assert "index" not in result_df.columns
    # The unique source node (0) has max distance 0.
    source_row = result_df[result_df["nodeId"] == 0].iloc[0]
    assert source_row["maxDistanceFromSource"] == 0
