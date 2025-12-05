from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.leiden_endpoints import LeidenWriteResult
from graphdatascience.procedure_surface.cypher.community.leiden_cypher_endpoints import LeidenCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
            (a:Person),
            (b:Person),
            (c:Person),
            (d:Person),
            (e:Person),
            (f:Person),
            (a)-[:KNOWS]->(b),
            (b)-[:KNOWS]->(c),
            (c)-[:KNOWS]->(d),
            (d)-[:KNOWS]->(e),
            (e)-[:KNOWS]->(f),
            (f)-[:KNOWS]->(a),
            (a)-[:KNOWS]->(c),
            (b)-[:KNOWS]->(d),
            (c)-[:KNOWS]->(e)
    """

    projection_query = """
        MATCH (n:Person)-[r:KNOWS]->(m:Person)
        WITH gds.graph.project("g", n, m, {relationshipType: "KNOWS"}, {undirectedRelationshipTypes: ["KNOWS"]}) as g
        RETURN g
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def leiden_endpoints(query_runner: QueryRunner) -> Generator[LeidenCypherEndpoints, None, None]:
    yield LeidenCypherEndpoints(query_runner)


def test_leiden_stats(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.stats(G=sample_graph, max_levels=10)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.community_count > 0
    assert result.ran_levels >= 1
    assert isinstance(result.did_converge, bool)
    assert isinstance(result.community_distribution, dict)
    assert isinstance(result.modularities, list)
    assert isinstance(result.modularity, float)
    assert result.node_count == 6


def test_leiden_stream(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result_df = leiden_endpoints.stream(
        G=sample_graph,
        max_levels=10,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df) == 6

    # Test with intermediate communities
    result_df_intermediate = leiden_endpoints.stream(
        G=sample_graph,
        max_levels=10,
        include_intermediate_communities=True,
    )

    assert "nodeId" in result_df_intermediate.columns
    assert "communityId" in result_df_intermediate.columns
    assert "intermediateCommunityIds" in result_df_intermediate.columns
    assert len(result_df_intermediate) == 6


def test_leiden_mutate(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.mutate(
        G=sample_graph,
        mutate_property="leiden_community",
        max_levels=10,
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6
    assert result.community_count > 0
    assert result.ran_levels >= 1
    assert isinstance(result.did_converge, bool)
    assert isinstance(result.modularities, list)
    assert isinstance(result.modularity, float)


def test_leiden_write(
    leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = leiden_endpoints.write(G=sample_graph, write_property="leiden_community", max_levels=10)

    assert isinstance(result, LeidenWriteResult)
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6
    assert result.community_count > 0
    assert result.ran_levels >= 1
    assert isinstance(result.did_converge, bool)
    assert isinstance(result.community_distribution, dict)
    assert isinstance(result.modularities, list)
    assert isinstance(result.modularity, float)

    # Verify the property was written to the database
    count_result = query_runner.run_cypher(
        "MATCH (n:Person) WHERE n.leiden_community IS NOT NULL RETURN COUNT(*) AS count"
    )
    assert count_result.squeeze() == 6


def test_leiden_estimate(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.estimate(sample_graph, max_levels=10)

    assert result.node_count == 6
    assert result.relationship_count == 18
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_leiden_with_consecutive_ids(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.stream(G=sample_graph, max_levels=10, consecutive_ids=True)

    assert "nodeId" in result.columns
    assert "communityId" in result.columns
    assert len(result) == 6


def test_leiden_with_min_community_size(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.stream(G=sample_graph, max_levels=10, min_community_size=2)

    assert "nodeId" in result.columns
    assert "communityId" in result.columns
    assert len(result) <= 6  # Some nodes might be filtered out due to min community size


def test_leiden_with_gamma_parameter(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.stats(G=sample_graph, max_levels=10, gamma=0.5)

    assert result.compute_millis >= 0
    assert result.community_count > 0
    assert isinstance(result.modularity, float)


def test_leiden_with_theta_parameter(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.stats(G=sample_graph, max_levels=10, theta=0.1)

    assert result.compute_millis >= 0
    assert result.community_count > 0
    assert isinstance(result.modularity, float)


def test_leiden_with_tolerance_parameter(leiden_endpoints: LeidenCypherEndpoints, sample_graph: GraphV2) -> None:
    result = leiden_endpoints.stats(G=sample_graph, max_levels=10, tolerance=1e-3)

    assert result.compute_millis >= 0
    assert result.community_count > 0
    assert isinstance(result.modularity, float)
