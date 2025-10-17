from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.clique_counting_endpoints import CliqueCountingWriteResult
from graphdatascience.procedure_surface.cypher.community.clique_counting_cypher_endpoints import (
    CliqueCountingCypherEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
        (a1)-[:T]->(a2)-[:T]->(a3)-[:T]->(a4), (a2)-[:T]->(a4)-[:T]->(a1)-[:T]->(a3)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipType: "T"}, {undirectedRelationshipTypes: ["T"]}) AS G
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
def clique_counting_endpoints(query_runner: QueryRunner) -> Generator[CliqueCountingCypherEndpoints, None, None]:
    yield CliqueCountingCypherEndpoints(query_runner)


def test_clique_counting_stats(clique_counting_endpoints: CliqueCountingCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test clique counting stats operation."""
    result = clique_counting_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.global_count == [4, 1]


def test_clique_counting_stream(
    clique_counting_endpoints: CliqueCountingCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test clique counting stream operation."""
    result_df = clique_counting_endpoints.stream(
        G=sample_graph,
    )

    assert set(result_df.columns) == {"nodeId", "counts"}
    assert len(result_df) == 4


def test_clique_counting_mutate(
    clique_counting_endpoints: CliqueCountingCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test clique counting mutate operation."""
    result = clique_counting_endpoints.mutate(
        G=sample_graph,
        mutate_property="cliqueCount",
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.global_count == [4, 1]


def test_clique_counting_write(
    clique_counting_endpoints: CliqueCountingCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = clique_counting_endpoints.write(G=sample_graph, write_property="cliqueCount")

    assert isinstance(result, CliqueCountingWriteResult)
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.global_count == [4, 1]

    assert query_runner.run_cypher("MATCH (n) WHERE n.cliqueCount IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 4


def test_clique_counting_estimate(
    clique_counting_endpoints: CliqueCountingCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = clique_counting_endpoints.estimate(sample_graph)

    assert result.node_count == 4
    assert result.relationship_count == 12
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
