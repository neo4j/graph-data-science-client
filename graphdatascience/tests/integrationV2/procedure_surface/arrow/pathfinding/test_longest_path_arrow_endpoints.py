from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pathfinding.longest_path_arrow_endpoints import LongestPathArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

# Create a DAG (Directed Acyclic Graph) for testing longest path
dag_graph = """
        CREATE
            (a: Node {id: 0}),
            (b: Node {id: 1}),
            (c: Node {id: 2}),
            (d: Node {id: 3}),
            (e: Node {id: 4}),
            (f: Node {id: 5}),
            (a)-[:LINK {cost: 1.0}]->(b),
            (a)-[:LINK {cost: 2.0}]->(c),
            (b)-[:LINK {cost: 3.0}]->(d),
            (c)-[:LINK {cost: 2.0}]->(d),
            (d)-[:LINK {cost: 1.0}]->(e),
            (c)-[:LINK {cost: 5.0}]->(f),
            (f)-[:LINK {cost: 1.0}]->(e)
        """


@pytest.fixture
def sample_dag(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "dag", dag_graph) as G:
        yield G


@pytest.fixture
def longest_path_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[LongestPathArrowEndpoints, None, None]:
    yield LongestPathArrowEndpoints(arrow_client)


def test_longest_path_stream(longest_path_endpoints: LongestPathArrowEndpoints, sample_dag: GraphV2) -> None:
    result_df = longest_path_endpoints.stream(
        G=sample_dag,
        relationship_weight_property="cost",
    )

    assert len(result_df) == 6
    assert {"index", "sourceNode", "targetNode", "totalCost", "nodeIds", "costs"} == set(result_df.columns)
