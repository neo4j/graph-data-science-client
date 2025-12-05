from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.clique_counting_endpoints import CliqueCountingWriteResult
from graphdatascience.procedure_surface.arrow.community.clique_counting_arrow_endpoints import (
    CliqueCountingArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a1)-[:T]->(a2)-[:T]->(a3)-[:T]->(a4), (a2)-[:T]->(a4)-[:T]->(a1)-[:T]->(a3)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph, ("T", "T2")) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
            MATCH (n)
            OPTIONAL MATCH (n)-->(m)
            WITH gds.graph.project.remote(n, m, {relationshipType: "T"}) as g
            RETURN g
        """,
        ["T"],
    ) as g:
        yield g


@pytest.fixture
def clique_counting_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[CliqueCountingArrowEndpoints, None, None]:
    yield CliqueCountingArrowEndpoints(arrow_client)


def test_clique_counting_stats(clique_counting_endpoints: CliqueCountingArrowEndpoints, sample_graph: GraphV2) -> None:
    result = clique_counting_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.global_count == [4, 1]


def test_clique_counting_stream(clique_counting_endpoints: CliqueCountingArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = clique_counting_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "cliqueCount" in result_df.columns
    assert len(result_df.columns) == 2


def test_clique_counting_mutate(clique_counting_endpoints: CliqueCountingArrowEndpoints, sample_graph: GraphV2) -> None:
    result = clique_counting_endpoints.mutate(
        G=sample_graph,
        mutate_property="cliqueCount",
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.global_count == [4, 1]


@pytest.mark.db_integration
def test_clique_counting_write(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    endpoints = CliqueCountingArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="cliqueCount")

    assert isinstance(result, CliqueCountingWriteResult)
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.global_count == [4, 1]

    assert query_runner.run_cypher("MATCH (n) WHERE n.cliqueCount IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 4


def test_clique_counting_estimate(
    clique_counting_endpoints: CliqueCountingArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = clique_counting_endpoints.estimate(sample_graph)

    assert result.node_count == 4
    assert result.relationship_count == 12
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
