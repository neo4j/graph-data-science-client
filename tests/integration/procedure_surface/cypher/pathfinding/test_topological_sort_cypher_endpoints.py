from typing import Generator

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.pathfinding.topological_sort_cypher_endpoints import (
    TopologicalSortCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integration.procedure_surface.cypher.cypher_graph_helper import create_graph

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
def sample_dag(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    projection_query = """
        MATCH (source)-[r]->(target)
        WITH gds.graph.project('dag', source, target, {
            sourceNodeProperties: properties(source),
            targetNodeProperties: properties(target),
            relationshipProperties: properties(r)
        }) AS G
        RETURN G
    """

    with create_graph(query_runner, "dag", dag_graph, projection_query) as G:
        yield G


@pytest.fixture
def topological_sort_endpoints(query_runner: QueryRunner) -> Generator[TopologicalSortCypherEndpoints, None, None]:
    yield TopologicalSortCypherEndpoints(query_runner)


def test_topological_sort_stream_with_max_distance(
    topological_sort_endpoints: TopologicalSortCypherEndpoints, sample_dag: Graph
) -> None:
    result_df = topological_sort_endpoints.stream(G=sample_dag, compute_max_distance_from_source=True)

    assert len(result_df) == 6
    assert {"nodeId", "maxDistanceFromSource"} == set(result_df.columns)
    assert result_df["maxDistanceFromSource"].notna().all()
