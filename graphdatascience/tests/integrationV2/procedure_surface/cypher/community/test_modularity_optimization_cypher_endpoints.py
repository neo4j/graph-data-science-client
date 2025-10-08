from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.modularity_optimization_cypher_endpoints import (
    ModularityOptimizationCypherEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (d: Node),
    (e: Node),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(e),
    (e)-[:REL]->(a)
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
def modularity_optimization_endpoints(
    query_runner: QueryRunner,
) -> Generator[ModularityOptimizationCypherEndpoints, None, None]:
    yield ModularityOptimizationCypherEndpoints(query_runner)


def test_modularity_optimization_stats(
    modularity_optimization_endpoints: ModularityOptimizationCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test Modularity Optimization stats operation."""
    result = modularity_optimization_endpoints.stats(G=sample_graph)

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.modularity is not None
    assert result.community_count > 0
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_modularity_optimization_stream(
    modularity_optimization_endpoints: ModularityOptimizationCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test Modularity Optimization stream operation."""
    result_df = modularity_optimization_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 5  # We have 5 nodes


def test_modularity_optimization_mutate(
    modularity_optimization_endpoints: ModularityOptimizationCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test Modularity Optimization mutate operation."""
    result = modularity_optimization_endpoints.mutate(
        G=sample_graph,
        mutate_property="communityId",
    )

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.modularity is not None
    assert result.community_count > 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.nodes == 5


def test_modularity_optimization_estimate(
    modularity_optimization_endpoints: ModularityOptimizationCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = modularity_optimization_endpoints.estimate(sample_graph)

    assert result.node_count == 5
    assert result.relationship_count == 5
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
