from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.cypher.catalog.node_properties_cypher_endpoints import (
    NodePropertiesCypherEndpoints,
)
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_query = """
        CREATE
        (a: Node {prop1: 1, prop2: 42.0}),
        (b: Node {prop1: 2, prop2: 43.0}),
        (c: Node {prop1: 3, prop2: 44.0})
    """

    projection_query = """
            MATCH (n)
            WITH gds.graph.project('g', n, null, {sourceNodeProperties: n {.prop1, .prop2}, targetNodeProperties: null}) AS G
            RETURN G
        """

    with create_graph(
        query_runner,
        "g",
        create_query,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def node_properties_endpoints(
    query_runner: QueryRunner,
) -> Generator[NodePropertiesCypherEndpoints, None, None]:
    yield NodePropertiesCypherEndpoints(query_runner)


def test_stream_node_properties(node_properties_endpoints: NodePropertiesCypherEndpoints, sample_graph: Graph) -> None:
    result = node_properties_endpoints.stream(G=sample_graph, node_properties=["prop1", "prop2"])

    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "prop1" in result.columns
    assert "prop2" in result.columns
    assert set(result["prop1"].tolist()) == {1, 2, 3}
    assert set(result["prop2"].tolist()) == {42.0, 43.0, 44.0}


def test_stream_node_properties_with_arrow(
    query_runner: QueryRunner, gds_arrow_client: GdsArrowClient, sample_graph: Graph
) -> None:
    endpoints = NodePropertiesCypherEndpoints(query_runner, gds_arrow_client)

    result = endpoints.stream(G=sample_graph, node_properties=["prop1", "prop2"])

    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "prop1" in result.columns
    assert "prop2" in result.columns
    assert set(result["prop1"].tolist()) == {1, 2, 3}
    assert set(result["prop2"].tolist()) == {42.0, 43.0, 44.0}


def test_stream_node_properties_with_labels(
    node_properties_endpoints: NodePropertiesCypherEndpoints, sample_graph: Graph
) -> None:
    result = node_properties_endpoints.stream(G=sample_graph, node_properties=["prop1"], list_node_labels=True)

    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "prop1" in result.columns
    assert "nodeLabels" in result.columns
    assert set(result["prop1"].tolist()) == {1, 2, 3}


def test_stream_node_properties_with_db_properties(
    node_properties_endpoints: NodePropertiesCypherEndpoints, sample_graph: Graph
) -> None:
    result = node_properties_endpoints.stream(G=sample_graph, node_properties=["prop1"], db_node_properties=["prop2"])

    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "prop1" in result.columns
    assert "prop2" in result.columns
    assert set(result["prop1"].tolist()) == {1, 2, 3}
    assert set(result["prop2"].tolist()) == {42.0, 43.0, 44.0}


def test_write_node_properties(
    node_properties_endpoints: NodePropertiesCypherEndpoints, sample_graph: Graph, query_runner: QueryRunner
) -> None:
    result = node_properties_endpoints.write(G=sample_graph, node_properties=["prop1", "prop2"])

    assert result.graph_name == sample_graph.name()
    assert result.node_properties == ["prop1", "prop2"]
    assert result.write_millis >= 0
    assert result.properties_written == 6  # 3 nodes * 2 properties

    # Verify properties were written to database
    node_written = query_runner.run_cypher("""
        MATCH (n:Node)
        WHERE n.prop1 IS NOT NULL AND n.prop2 IS NOT NULL
        RETURN COUNT(n) as written
    """).squeeze()

    assert node_written == 3


def test_drop_node_properties(node_properties_endpoints: NodePropertiesCypherEndpoints, sample_graph: Graph) -> None:
    # Drop one property
    drop_result = node_properties_endpoints.drop(G=sample_graph, node_properties=["prop1"])

    assert drop_result.graph_name == sample_graph.name()
    assert drop_result.node_properties == ["prop1"]
    assert drop_result.properties_removed == 3  # 3 nodes * 1 property


def test_drop_multiple_node_properties(
    node_properties_endpoints: NodePropertiesCypherEndpoints, sample_graph: Graph
) -> None:
    # Drop both properties
    drop_result = node_properties_endpoints.drop(G=sample_graph, node_properties=["prop1", "prop2"])

    assert drop_result.graph_name == sample_graph.name()
    assert drop_result.node_properties == ["prop1", "prop2"]
    assert drop_result.properties_removed == 6  # 3 nodes * 2 properties
