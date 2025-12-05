from typing import Generator

import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.longest_path_cypher_endpoints import (
    LongestPathCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph

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
def sample_dag(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
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
def longest_path_endpoints(query_runner: QueryRunner) -> Generator[LongestPathCypherEndpoints, None, None]:
    yield LongestPathCypherEndpoints(query_runner)


def test_longest_path_stream(longest_path_endpoints: LongestPathCypherEndpoints, sample_dag: GraphV2) -> None:
    result_df = longest_path_endpoints.stream(
        G=sample_dag,
        relationship_weight_property="cost",
    )

    assert len(result_df) == 6
    assert {"index", "sourceNode", "targetNode", "totalCost", "nodeIds", "costs"} == set(result_df.columns)
