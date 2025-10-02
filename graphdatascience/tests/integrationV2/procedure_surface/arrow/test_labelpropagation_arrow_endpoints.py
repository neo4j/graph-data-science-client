from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.labelpropagation_endpoints import LabelPropagationWriteResult
from graphdatascience.procedure_surface.arrow.labelpropagation_arrow_endpoints import LabelPropagationArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

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


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(
        arrow_client,
        "g",
        create_statement,
    ) as g:
        yield g


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        create_statement,
        """
            MATCH (n)-[r]->(m)
            WITH gds.graph.project.remote(n, m) as g
            RETURN g
        """,
    ) as g:
        yield g


@pytest.fixture
def labelpropagation_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[LabelPropagationArrowEndpoints, None, None]:
    yield LabelPropagationArrowEndpoints(arrow_client)


def test_labelpropagation_stats(
    labelpropagation_endpoints: LabelPropagationArrowEndpoints, sample_graph: GraphV2
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
    labelpropagation_endpoints: LabelPropagationArrowEndpoints, sample_graph: GraphV2
) -> None:
    result_df = labelpropagation_endpoints.stream(
        G=sample_graph,
        max_iterations=10,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df) == 4


def test_labelpropagation_mutate(
    labelpropagation_endpoints: LabelPropagationArrowEndpoints, sample_graph: GraphV2
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


@pytest.mark.db_integration
def test_labelpropagation_write(
    arrow_client: AuthenticatedArrowClient, db_graph: GraphV2, query_runner: QueryRunner
) -> None:
    endpoints = LabelPropagationArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))

    result = endpoints.write(G=db_graph, write_property="lp_community", max_iterations=10)

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

    assert query_runner.run_cypher("MATCH (n) WHERE n.lp_community IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 4


def test_labelpropagation_estimate(
    labelpropagation_endpoints: LabelPropagationArrowEndpoints, sample_graph: GraphV2
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
    labelpropagation_endpoints: LabelPropagationArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = labelpropagation_endpoints.stream(G=sample_graph, max_iterations=10, consecutive_ids=True)

    assert "nodeId" in result.columns
    assert "communityId" in result.columns
    assert len(result) == 4


def test_labelpropagation_with_min_community_size(
    labelpropagation_endpoints: LabelPropagationArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = labelpropagation_endpoints.stream(G=sample_graph, max_iterations=10, min_community_size=2)

    assert "nodeId" in result.columns
    assert "communityId" in result.columns
    assert len(result) <= 4  # Some nodes might be filtered out due to min community size
