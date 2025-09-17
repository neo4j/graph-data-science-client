from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.k1coloring_cypher_endpoints import K1ColoringCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (a)-[:REL]->(c)
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
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def k1coloring_endpoints(query_runner: QueryRunner) -> Generator[K1ColoringCypherEndpoints, None, None]:
    yield K1ColoringCypherEndpoints(query_runner)


def test_k1coloring_stats(k1coloring_endpoints: K1ColoringCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K1Coloring stats operation."""
    result = k1coloring_endpoints.stats(G=sample_graph)

    assert result.color_count >= 1
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.ran_iterations >= 0
    assert isinstance(result.did_converge, bool)


def test_k1coloring_stream(k1coloring_endpoints: K1ColoringCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K1Coloring stream operation."""
    result_df = k1coloring_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "color" in result_df.columns
    assert len(result_df.columns) == 2


def test_k1coloring_mutate(k1coloring_endpoints: K1ColoringCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K1Coloring mutate operation."""
    result = k1coloring_endpoints.mutate(
        G=sample_graph,
        mutate_property="color",
    )

    assert result.color_count >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_count == 3
    assert result.ran_iterations >= 0
    assert isinstance(result.did_converge, bool)


def test_k1coloring_estimate(k1coloring_endpoints: K1ColoringCypherEndpoints, sample_graph: GraphV2) -> None:
    result = k1coloring_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 1
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
