from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.arrow.arrow_wcc_endpoints import WccArrowEndpoints
from graphdatascience.procedure_surface.cypher.wcc_cypher_endpoints import WccCypherEndpoints


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (a)-[:REL]->(c)
    """

    query_runner.run_cypher(create_statement)

    query_runner.run_cypher("""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
        RETURN G
    """)

    yield Graph("g", query_runner)

    query_runner.run_cypher("CALL gds.graph.drop('g')")
    query_runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def wcc_endpoints(query_runner: QueryRunner) -> Generator[WccCypherEndpoints, None, None]:
    yield WccCypherEndpoints(query_runner)


def test_wcc_stats(wcc_endpoints: WccArrowEndpoints, sample_graph: Graph) -> None:
    """Test WCC stats operation."""
    result = wcc_endpoints.stats(G=sample_graph)

    assert result.component_count == 2
    assert result.compute_millis > 0
    assert result.pre_processing_millis > 0
    assert result.post_processing_millis > 0
    assert "p10" in result.component_distribution


def test_wcc_stream(wcc_endpoints: WccArrowEndpoints, sample_graph: Graph) -> None:
    """Test WCC stream operation."""
    result_df = wcc_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "componentId" in result_df.columns
    assert len(result_df.columns) == 2


def test_wcc_mutate(wcc_endpoints: WccArrowEndpoints, sample_graph: Graph) -> None:
    """Test WCC mutate operation."""
    result = wcc_endpoints.mutate(
        G=sample_graph,
        mutate_property="componentId",
    )

    assert result.component_count == 2
    assert "p10" in result.component_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3
