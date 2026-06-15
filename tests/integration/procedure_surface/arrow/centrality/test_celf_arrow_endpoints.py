from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.centrality.celf_endpoints import CelfWriteResult
from graphdatascience.procedure_surface.arrow.centrality.celf_arrow_endpoints import CelfArrowEndpoints
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a:Node {id: 0}),
            (b:Node {id: 1}),
            (c:Node {id: 2}),
            (d:Node {id: 3}),
            (e:Node {id: 4}),
            (a)-[:REL]->(b),
            (a)-[:REL]->(c),
            (b)-[:REL]->(d),
            (c)-[:REL]->(e),
            (d)-[:REL]->(e)
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
                    WITH gds.graph.project.remote(n, m) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def celf_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[CelfArrowEndpoints, None, None]:
    yield CelfArrowEndpoints(arrow_client)


def test_celf_stats(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    result = celf_endpoints.stats(G=sample_graph, seed_set_size=2)

    assert result.compute_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert isinstance(result.configuration, dict)


def test_celf_stream(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    result_df = celf_endpoints.stream(G=sample_graph, seed_set_size=2)

    assert set(result_df.columns) == {"nodeId", "spread"}
    assert len(result_df) == 5  # same as node count
    assert all(result_df["spread"] >= 0)


def test_celf_mutate(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    result = celf_endpoints.mutate(G=sample_graph, seed_set_size=2, mutate_property="celf_spread")

    assert result.node_properties_written == 5  # All nodes get properties (influence spread values)
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert isinstance(result.configuration, dict)


@pytest.mark.db_integration
def test_celf_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph) -> None:
    endpoints = CelfArrowEndpoints(arrow_client, WriteProtocol.select(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, seed_set_size=2, write_property="celf_spread")

    assert isinstance(result, CelfWriteResult)
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert result.node_properties_written == 5

    assert (
        query_runner.run_cypher(
            "MATCH (n) WHERE n.celf_spread IS NOT NULL RETURN COUNT(*) AS count", query_type=QueryType.USER_ACTION
        ).squeeze()
        == 5
    )


def test_celf_write_without_write_back_client(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    with pytest.raises(Exception, match="Write back is not supported by this session."):
        celf_endpoints.write(
            G=sample_graph,
            seed_set_size=2,
            write_property="celf_spread",
        )


def test_celf_estimate(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    result = celf_endpoints.estimate(G=sample_graph, seed_set_size=2)

    assert result.node_count == 5
    assert result.relationship_count >= 0
    assert "Bytes" in result.required_memory or "KiB" in result.required_memory or "MiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_compute(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    handle = celf_endpoints.compute(G=sample_graph, seed_set_size=2)
    summary = handle.summary()

    assert summary["computeMillis"] >= 0
    assert summary["totalSpread"] >= 0.0
    assert summary["nodeCount"] == 5
    assert "writeProperty" not in summary["configuration"]

    df = handle.stream()
    assert set(df.columns) == {"nodeId", "spread"}
    assert len(df) == 5
