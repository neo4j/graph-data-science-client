from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.write_back_client import WriteBackClient
from graphdatascience.procedure_surface.arrow.catalog.node_properties_arrow_endpoints import (
    NodePropertiesArrowEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
        CREATE
        (a: Node {prop1: 1, prop2: 42.0}),
        (b: Node {prop1: 2, prop2: 43.0}),
        (c: Node {prop1: 3, prop2: 44.0})
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    graph_data = """
        CREATE
        (a: Node {prop1: 1, prop2: 42.0}),
        (b: Node {prop1: 2, prop2: 43.0}),
        (c: Node {prop1: 3, prop2: 44.0})
    """

    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph_data,
        "MATCH (n) WITH gds.graph.project.remote(n, null, {sourceNodeLabels: labels(n), targetNodeLabels: null, sourceNodeProperties: properties(n)}) as g RETURN g",
    ) as G:
        yield G


@pytest.fixture
def node_properties_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[NodePropertiesArrowEndpoints, None, None]:
    yield NodePropertiesArrowEndpoints(arrow_client)


@pytest.fixture
def node_properties_endpoints_with_db(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> Generator[NodePropertiesArrowEndpoints, None, None]:
    yield NodePropertiesArrowEndpoints(arrow_client, WriteBackClient(arrow_client, query_runner))


def test_stream_node_properties(node_properties_endpoints: NodePropertiesArrowEndpoints, sample_graph: Graph) -> None:
    result = node_properties_endpoints.stream(G=sample_graph, node_properties=["prop1", "prop2"])

    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "prop1" in result.columns
    assert "prop2" in result.columns
    assert set(result["prop1"].tolist()) == {1, 2, 3}
    assert set(result["prop2"].tolist()) == {42.0, 43.0, 44.0}


def test_stream_node_properties_with_labels(
    node_properties_endpoints: NodePropertiesArrowEndpoints, sample_graph: Graph
) -> None:
    result = node_properties_endpoints.stream(G=sample_graph, node_properties=["prop1"], list_node_labels=True)

    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "prop1" in result.columns
    assert "labels" in result.columns
    assert set(result["prop1"].tolist()) == {1, 2, 3}


@pytest.mark.db_integration
def test_write_node_properties(
    node_properties_endpoints_with_db: NodePropertiesArrowEndpoints, db_graph: Graph, query_runner: QueryRunner
) -> None:
    result = node_properties_endpoints_with_db.write(G=db_graph, node_properties=["prop1", "prop2"])

    assert result.graph_name == db_graph.name()
    assert result.node_properties == ["prop1", "prop2"]
    assert result.write_millis >= 0
    assert result.properties_written == 6  # 3 nodes * 2 properties

    # Verify properties were written to database
    props_written = query_runner.run_cypher("""
        MATCH (n:Node)
        WHERE n.prop1 IS NOT NULL AND n.prop2 IS NOT NULL
        RETURN COUNT(n) as written
    """).squeeze()

    assert props_written == 3


def test_drop_node_properties(node_properties_endpoints: NodePropertiesArrowEndpoints, sample_graph: Graph) -> None:
    # Drop one property
    drop_result = node_properties_endpoints.drop(G=sample_graph, node_properties=["prop1"])

    assert drop_result.graph_name == sample_graph.name()
    assert drop_result.node_properties == ["prop1"]
    assert drop_result.properties_removed == 3  # 3 nodes * 1 property


def test_drop_multiple_node_properties(
    node_properties_endpoints: NodePropertiesArrowEndpoints, sample_graph: Graph
) -> None:
    # Drop both properties
    drop_result = node_properties_endpoints.drop(G=sample_graph, node_properties=["prop1", "prop2"])

    assert drop_result.graph_name == sample_graph.name()
    assert drop_result.node_properties == ["prop1", "prop2"]
    assert drop_result.properties_removed == 6  # 3 nodes * 2 properties
