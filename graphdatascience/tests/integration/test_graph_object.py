from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

GRAPH_NAME = "g"


@pytest.fixture(scope="class", autouse=True)
def setup_module(runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    runner.run_query(
        """
        CREATE
        (a: Node {x: 1}),
        (b: Node {x: 2}),
        (c: Node {x: 3}),
        (a)-[:REL {y: 42.0}]->(b),
        (a)-[:REL {y: 13.37}]->(c),
        (b)-[:REL {z: 7.9}]->(c)
        """
    )

    yield

    runner.run_query("MATCH (n) DETACH DELETE n")


@pytest.fixture
def G(gds: GraphDataScience) -> Generator[Graph, None, None]:
    G = gds.graph.project(
        GRAPH_NAME, {"Node": {"properties": "x"}}, {"REL": {"properties": ["y", "z"]}}
    )
    yield G

    gds.graph.drop(G, False)


def test_graph_node_count(G: Graph) -> None:
    assert G.node_count() == 3


def test_graph_relationship_count(G: Graph) -> None:
    assert G.relationship_count() == 3


def test_graph_node_properties(G: Graph) -> None:
    assert G.node_properties("Node") == ["x"]


def test_graph_relationship_properties(G: Graph) -> None:
    assert G.relationship_properties("REL") == ["y", "z"]


def test_graph_degree_distribution(G: Graph) -> None:
    assert G.degree_distribution()["mean"] == 2.0


def test_graph_density(G: Graph) -> None:
    assert G.density() == 0.5


def test_graph_memory_usage(G: Graph) -> None:
    assert G.memory_usage()


def test_graph_size_in_bytes(G: Graph) -> None:
    assert G.size_in_bytes() > 0


def test_graph_exists(gds: GraphDataScience, G: Graph) -> None:
    assert G.exists()

    gds.graph.drop(G)

    assert not G.exists()


def test_graph_drop(G: Graph) -> None:
    assert G.exists()

    G.drop()

    assert not G.exists()

    with pytest.raises(ValueError):
        G.node_count()
