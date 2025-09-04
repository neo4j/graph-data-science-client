from typing import Generator

import pytest

from graphdatascience import QueryRunner, Graph
from graphdatascience.procedure_surface.cypher.catalog.node_label_cypher_endpoints import NodeLabelCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import delete_all_graphs


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node:Foo),
    (b: Node),
    (c: Node:Foo)
    """

    query_runner.run_cypher(create_statement)

    query_runner.run_cypher("""
        MATCH (n)
        WITH gds.graph.project('g', n, null, {sourceNodeLabels: labels(n)}) AS G
        RETURN G
    """)

    yield Graph("g", query_runner)

    delete_all_graphs(query_runner)
    query_runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def node_label_endpoints(query_runner: QueryRunner) -> Generator[NodeLabelCypherEndpoints, None, None]:
    yield NodeLabelCypherEndpoints(query_runner)

@pytest.fixture
def test_mutate_node_label(endpoint: NodeLabelCypherEndpoints, sample_graph: Graph) -> None:
    result = endpoint.mutate(G=sample_graph, node_label="MUTATED", node_filter="n:Foo")

    assert result.node_label == "MUTATED"
    assert result.node_count == 2
    assert result.graph_name == sample_graph.name()
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 2

    assert "MUTATED" in sample_graph.node_labels()
