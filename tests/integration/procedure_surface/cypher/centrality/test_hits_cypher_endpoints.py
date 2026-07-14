from typing import Generator

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.centrality.hits_cypher_endpoints import HitsCypherEndpoints
from graphdatascience.query_runner import QueryRunner
from tests.integration.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_query = """
        CREATE
        (a: Node),
        (b: Node),
        (c: Node),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_query,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def hits_endpoints(query_runner: QueryRunner) -> Generator[HitsCypherEndpoints, None, None]:
    yield HitsCypherEndpoints(query_runner)


def test_hits_stats(hits_endpoints: HitsCypherEndpoints, sample_graph: Graph) -> None:
    result = hits_endpoints.stats(G=sample_graph, hits_iterations=10)

    assert result.ran_iterations > 0
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0


def test_hits_stream(hits_endpoints: HitsCypherEndpoints, sample_graph: Graph) -> None:
    result_df = hits_endpoints.stream(G=sample_graph, hits_iterations=10)

    assert "nodeId" in result_df.columns
    assert "values" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 3


def test_hits_mutate(hits_endpoints: HitsCypherEndpoints, sample_graph: Graph) -> None:
    result = hits_endpoints.mutate(G=sample_graph, mutate_property="hits", hits_iterations=10)

    assert result.ran_iterations > 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written > 0


def test_hits_write(hits_endpoints: HitsCypherEndpoints, sample_graph: Graph) -> None:
    result = hits_endpoints.write(G=sample_graph, write_property="hits", hits_iterations=10)

    assert result.ran_iterations > 0
    assert result.write_millis >= 0
    assert result.node_properties_written > 0
