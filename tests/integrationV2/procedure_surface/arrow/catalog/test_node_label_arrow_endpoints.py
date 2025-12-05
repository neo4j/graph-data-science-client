from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.catalog.node_label_arrow_endpoints import NodeLabelArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    gdl = """
        CREATE
        (a: Node:Foo),
        (b: Node),
        (c: Node:Foo)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    graph_data = """
        CREATE
        (a: Node:Foo),
        (b: Node),
        (c: Node:Foo)
    """

    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph_data,
        "MATCH (n) WITH gds.graph.project.remote(n, null, {sourceNodeLabels: labels(n), targetNodeLabels: null}) as g RETURN g",
    ) as G:
        yield G


@pytest.fixture
def node_label_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[NodeLabelArrowEndpoints, None, None]:
    yield NodeLabelArrowEndpoints(arrow_client)


@pytest.fixture
def node_label_endpoints_with_db(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> Generator[NodeLabelArrowEndpoints, None, None]:
    yield NodeLabelArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))


def test_mutate_node_label(node_label_endpoints: NodeLabelArrowEndpoints, sample_graph: GraphV2) -> None:
    result = node_label_endpoints.mutate(G=sample_graph, node_label="MUTATED", node_filter="n:Foo")

    assert result.node_label == "MUTATED"
    assert result.node_count == 3
    assert result.graph_name == sample_graph.name()
    assert result.mutate_millis >= 0
    assert result.node_labels_written == 2


@pytest.mark.db_integration
def test_write_node_label(
    node_label_endpoints_with_db: NodeLabelArrowEndpoints, db_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = node_label_endpoints_with_db.write(G=db_graph, node_label="WRITTEN", node_filter="n:Foo")

    assert result.node_label == "WRITTEN"
    assert result.node_count == 3
    assert result.graph_name == db_graph.name()
    assert result.write_millis >= 0
    assert result.node_labels_written == 2

    labels_written = query_runner.run_cypher("""
        MATCH (n:WRITTEN)
        RETURN COUNT(n) as written
    """).squeeze()

    assert labels_written == 2
