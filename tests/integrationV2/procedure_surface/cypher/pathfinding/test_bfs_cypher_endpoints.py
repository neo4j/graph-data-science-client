from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.bfs_cypher_endpoints import BFSCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_id


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {id: 0}),
    (b: Node {id: 1}),
    (c: Node {id: 2}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def bfs_endpoints(query_runner: QueryRunner) -> Generator[BFSCypherEndpoints, None, None]:
    yield BFSCypherEndpoints(query_runner)


def test_bfs_stream(bfs_endpoints: BFSCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner) -> None:
    result_df = bfs_endpoints.stream(G=sample_graph, source_node=find_node_by_id(query_runner, 0))

    assert set(result_df.columns) >= {"sourceNode", "nodeIds"}
    assert len(result_df) == 1


def test_bfs_mutate(bfs_endpoints: BFSCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner) -> None:
    result = bfs_endpoints.mutate(
        G=sample_graph, source_node=find_node_by_id(query_runner, 0), mutate_relationship_type="BFS_REL"
    )

    assert result.relationships_written == 2


def test_bfs_estimate(bfs_endpoints: BFSCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner) -> None:
    result = bfs_endpoints.estimate(sample_graph, source_node=find_node_by_id(query_runner, 0))

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory


def test_bfs_stats(bfs_endpoints: BFSCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner) -> None:
    result = bfs_endpoints.stats(G=sample_graph, source_node=find_node_by_id(query_runner, 0))

    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
