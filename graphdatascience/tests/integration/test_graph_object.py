from typing import Generator

import pytest
from pandas import Series

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
        (d: Node2 {s: 4}),
        (a)-[:REL {y: 42.0}]->(b),
        (a)-[:REL {y: 13.37}]->(c),
        (b)-[:REL {z: 7.9}]->(c),
        (b)-[:REL2 {q: 7.9}]->(d)
        """
    )

    yield

    runner.run_query("MATCH (n) DETACH DELETE n")


@pytest.fixture
def G(gds: GraphDataScience) -> Generator[Graph, None, None]:
    G, _ = gds.graph.project(
        GRAPH_NAME,
        {"Node": {"properties": "x"}, "Node2": {"properties": "s"}},
        {"REL": {"properties": ["y", "z"]}, "REL2": {"properties": "q"}},
    )

    yield G

    gds.graph.drop(G, False)


def test_graph_database(gds: GraphDataScience, G: Graph) -> None:
    assert G.database() == gds.database()


def test_graph_configuration(G: Graph) -> None:
    assert "Node" in G.configuration()["nodeProjection"].keys()


def test_graph_node_count(G: Graph) -> None:
    assert G.node_count() == 4


def test_graph_relationship_count(G: Graph) -> None:
    assert G.relationship_count() == 4


def test_graph_node_labels(G: Graph) -> None:
    assert set(G.node_labels()) == {"Node", "Node2"}


def test_graph_relationship_types(G: Graph) -> None:
    assert set(G.relationship_types()) == {"REL", "REL2"}


def test_graph_node_properties(G: Graph) -> None:
    assert G.node_properties("Node") == ["x"]
    assert G.node_properties("Node2") == ["s"]

    node_properties = G.node_properties()
    assert isinstance(node_properties, Series)
    assert node_properties.size == 2
    assert node_properties["Node"] == ["x"]
    assert node_properties["Node2"] == ["s"]


def test_graph_relationship_properties(G: Graph) -> None:
    assert set(G.relationship_properties("REL")) == {"y", "z"}
    assert G.relationship_properties("REL2") == ["q"]

    rel_properties = G.relationship_properties()
    assert isinstance(rel_properties, Series)
    assert rel_properties.size == 2
    assert set(rel_properties["REL"]) == {"y", "z"}
    assert rel_properties["REL2"] == ["q"]


def test_graph_degree_distribution(G: Graph) -> None:
    assert G.degree_distribution()["mean"] == 1.75


def test_graph_density(G: Graph) -> None:
    assert G.density() == pytest.approx(0.333, 0.01)


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

    assert G.drop()["graphName"] == G.name()

    assert not G.exists()

    with pytest.raises(ValueError):
        G.node_count()

    # Should not raise error
    G.drop(failIfMissing=False)

    with pytest.raises(Exception):
        G.drop(failIfMissing=True)


def test_graph_creation_time(G: Graph) -> None:
    assert G.creation_time().year > 2000


def test_graph_modification_time(G: Graph) -> None:
    assert G.modification_time().year > 2000


def test_graph_str(G: Graph) -> None:
    assert str(G) == "Graph(name=g, node_count=4, relationship_count=4)"


def test_graph_repr(G: Graph) -> None:
    assert "'memoryUsage'" in repr(G)
