from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.labelpropagation_endpoints import LabelPropagationWriteResult
from graphdatascience.procedure_surface.cypher.community.labelpropagation_cypher_endpoints import (
    LabelPropagationCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
            (a:Person),
            (b:Person),
            (c:Person),
            (d:Person),
            (a)-[:KNOWS]->(b),
            (b)-[:KNOWS]->(a),
            (c)-[:KNOWS]->(d),
            (d)-[:KNOWS]->(c),
            (b)-[:KNOWS]->(c)
    """

    projection_query = """
        MATCH (n:Person)-[r:KNOWS]->(m:Person)
        WITH gds.graph.project("g", n, m) as g
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
def labelpropagation_endpoints(query_runner: QueryRunner) -> Generator[LabelPropagationCypherEndpoints, None, None]:
    yield LabelPropagationCypherEndpoints(query_runner)


def test_labelpropagation_stats(
    labelpropagation_endpoints: LabelPropagationCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = labelpropagation_endpoints.stats(G=sample_graph, max_iterations=10)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.community_count > 0
    assert result.ran_iterations >= 1
    assert isinstance(result.did_converge, bool)
    assert isinstance(result.community_distribution, dict)


def test_labelpropagation_stream(
    labelpropagation_endpoints: LabelPropagationCypherEndpoints, sample_graph: GraphV2
) -> None:
    result_df = labelpropagation_endpoints.stream(
        G=sample_graph,
        max_iterations=10,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df) == 4


def test_labelpropagation_mutate(
    labelpropagation_endpoints: LabelPropagationCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = labelpropagation_endpoints.mutate(
        G=sample_graph,
        mutate_property="lp_community",
        max_iterations=10,
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.community_count > 0
    assert result.ran_iterations >= 1
    assert isinstance(result.did_converge, bool)


def test_labelpropagation_write(
    labelpropagation_endpoints: LabelPropagationCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = labelpropagation_endpoints.write(G=sample_graph, write_property="lp_community", max_iterations=10)

    assert isinstance(result, LabelPropagationWriteResult)
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.community_count > 0
    assert result.ran_iterations >= 1
    assert isinstance(result.did_converge, bool)
    assert isinstance(result.community_distribution, dict)

    # Verify the property was written to the database
    count_result = query_runner.run_cypher("MATCH (n:Person) WHERE n.lp_community IS NOT NULL RETURN COUNT(*) AS count")
    assert count_result.squeeze() == 4


def test_labelpropagation_estimate(
    labelpropagation_endpoints: LabelPropagationCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = labelpropagation_endpoints.estimate(sample_graph, max_iterations=10)

    assert result.node_count == 4
    assert result.relationship_count == 5
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_labelpropagation_with_consecutive_ids(
    labelpropagation_endpoints: LabelPropagationCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = labelpropagation_endpoints.stream(G=sample_graph, max_iterations=10, consecutive_ids=True)

    assert "nodeId" in result.columns
    assert "communityId" in result.columns
    assert len(result) == 4


def test_labelpropagation_with_min_community_size(
    labelpropagation_endpoints: LabelPropagationCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = labelpropagation_endpoints.stream(G=sample_graph, max_iterations=10, min_community_size=2)

    assert "nodeId" in result.columns
    assert "communityId" in result.columns
    assert len(result) <= 4  # Some nodes might be filtered out due to min community size
