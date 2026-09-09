from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.centrality.hits_endpoints import (
    HitsMutateResult,
    HitsStatsResult,
    HitsWriteResult,
)
from graphdatascience.procedure_surface.arrow.centrality.hits_arrow_endpoints import HitsArrowEndpoints
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

# HITS requires a directed graph.
graph = """
    CREATE
    (a:Node),
    (b:Node),
    (c:Node),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)-->(m)
                    WITH gds.graph.project.remote(n, m, {relationshipType: "REL"}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def hits_endpoints(arrow_client: AuthenticatedArrowClient) -> HitsArrowEndpoints:
    return HitsArrowEndpoints(arrow_client)


def test_hits_stats(hits_endpoints: HitsArrowEndpoints, sample_graph: Graph) -> None:
    result = hits_endpoints.stats(sample_graph, hits_iterations=5)

    assert isinstance(result, HitsStatsResult)
    assert result.ran_iterations >= 0
    assert isinstance(result.did_converge, bool)
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0


def test_hits_stream(hits_endpoints: HitsArrowEndpoints, sample_graph: Graph) -> None:
    result_df = hits_endpoints.stream(sample_graph, hits_iterations=5)

    assert set(result_df.columns) == {"nodeId", "hub", "auth"}
    assert len(result_df) == 3


def test_hits_mutate(hits_endpoints: HitsArrowEndpoints, sample_graph: Graph) -> None:
    result = hits_endpoints.mutate(sample_graph, mutate_property="Score", hits_iterations=5)

    assert isinstance(result, HitsMutateResult)
    assert result.ran_iterations >= 0
    assert isinstance(result.did_converge, bool)
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    # HITS writes two node properties (hub + auth) per node.
    assert result.node_properties_written == 6


@pytest.mark.db_integration
def test_hits_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph) -> None:
    endpoints = HitsArrowEndpoints(arrow_client, WriteProtocol.select(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="Score", hits_iterations=5)

    assert isinstance(result, HitsWriteResult)
    assert result.ran_iterations >= 0
    assert isinstance(result.did_converge, bool)
    assert result.write_millis >= 0
    # HITS writes two node properties (hub + auth) per node.
    assert result.node_properties_written == 6

    # The two properties are named <hub_property|auth_property> + write_property.
    assert (
        query_runner.run_cypher(
            "MATCH (n) WHERE n.hubScore IS NOT NULL AND n.authScore IS NOT NULL RETURN COUNT(*) AS count",
            query_type=QueryType.USER_ACTION,
        ).squeeze()
        == 3
    )


def test_hits_estimate(hits_endpoints: HitsArrowEndpoints, sample_graph: Graph) -> None:
    result = hits_endpoints.estimate(sample_graph, hits_iterations=5)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_compute(hits_endpoints: HitsArrowEndpoints, sample_graph: Graph) -> None:
    handle = hits_endpoints.compute(G=sample_graph, hits_iterations=5)
    summary = handle.summary()

    assert summary["ranIterations"] >= 0
    assert summary["computeMillis"] >= 0
    assert "writeProperty" not in summary["configuration"]

    df = handle.stream()
    assert "nodeId" in df.columns
