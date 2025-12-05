from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.catalog.node_label_cypher_endpoints import NodeLabelCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import (
    create_graph,
)


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_query = """
        CREATE
        (a: Node:Foo),
        (b: Node),
        (c: Node:Foo)
    """

    projection_query = """
        MATCH (n)
        WITH gds.graph.project('g', n, null, {sourceNodeLabels: labels(n), targetNodeLabels: null}) AS G
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
def node_label_endpoints(query_runner: QueryRunner) -> Generator[NodeLabelCypherEndpoints, None, None]:
    yield NodeLabelCypherEndpoints(query_runner)


def test_mutate_node_label(node_label_endpoints: NodeLabelCypherEndpoints, sample_graph: GraphV2) -> None:
    result = node_label_endpoints.mutate(G=sample_graph, node_label="MUTATED", node_filter="n:Foo")

    assert result.node_label == "MUTATED"
    assert result.node_count == 3
    assert result.graph_name == sample_graph.name()
    assert result.mutate_millis >= 0
    assert result.node_labels_written == 2

    assert "MUTATED" in sample_graph.node_labels()


def test_write_node_label(
    node_label_endpoints: NodeLabelCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = node_label_endpoints.write(G=sample_graph, node_label="WRITTEN", node_filter="n:Foo")

    assert result.node_label == "WRITTEN"
    assert result.node_count == 3
    assert result.graph_name == sample_graph.name()
    assert result.write_millis >= 0
    assert result.node_labels_written == 2

    assert "MUTATED" not in sample_graph.node_labels()

    assert (
        query_runner.run_cypher("""
        MATCH (n:WRITTEN)
        RETURN COUNT(n) as written
    """).squeeze()
        == 2
    )
