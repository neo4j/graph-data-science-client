from typing import Generator

import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def G(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_query = """
    CREATE
        (a: Node {x: 1}),
        (b: Node {x: 2}),
        (c: Node {x: 3}),
        (d: Node2 {s: 4}),
        (a)-[:REL {y: 42.0, z: 1}]->(b),
        (a)-[:REL {y: 13.37, z: 2}]->(c),
        (b)-[:REL {z: 7.9, z: 3}]->(c),
        (b)-[:REL2 {q: 7.9}]->(d)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {
            sourceNodeLabels: labels(n),
            sourceNodeProperties: properties(n),
            targetNodeProperties: properties(m),
            targetNodeLabels: labels(m),
            relationshipType: type(r),
            relationshipProperties: properties(r)
        }) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_query,
        projection_query,
    ) as g:
        yield g


def test_graph_configuration(G: GraphV2) -> None:
    assert "jobId" in G.configuration().keys()


def test_graph_node_count(G: GraphV2) -> None:
    assert G.node_count() == 4


def test_graph_relationship_count(G: GraphV2) -> None:
    assert G.relationship_count() == 4


def test_graph_node_labels(G: GraphV2) -> None:
    assert set(G.node_labels()) == {"Node", "Node2"}


def test_graph_relationship_types(G: GraphV2) -> None:
    assert set(G.relationship_types()) == {"REL", "REL2"}


def test_graph_node_properties(G: GraphV2) -> None:
    node_properties = G.node_properties()
    assert isinstance(node_properties, dict)
    assert node_properties == {"Node": ["x"], "Node2": ["s"]}


def test_graph_relationship_properties(G: GraphV2) -> None:
    rel_properties = G.relationship_properties()
    assert isinstance(rel_properties, dict)
    assert len(rel_properties.keys()) == 2
    assert set(rel_properties["REL"]) == {"y", "z"}
    assert rel_properties["REL2"] == ["q"]


def test_graph_degree_distribution(G: GraphV2) -> None:
    assert G.degree_distribution()["mean"] == 1.75


def test_graph_density(G: GraphV2) -> None:
    assert G.density() == pytest.approx(0.333, 0.01)


def test_graph_memory_usage(G: GraphV2) -> None:
    assert G.memory_usage()


def test_graph_size_in_bytes(G: GraphV2) -> None:
    assert G.size_in_bytes() > 0


def test_graph_exists(G: GraphV2) -> None:
    assert G.exists()

    G.drop()

    assert not G.exists()


def test_graph_drop(G: GraphV2) -> None:
    assert G.exists()

    result = G.drop()
    assert result and result.graph_name == G.name()

    assert not G.exists()

    with pytest.raises(ValueError):
        G.node_count()

    # Should not raise error
    G.drop(failIfMissing=False)

    with pytest.raises(Exception, match="Graph with name `g` does not exist on database `neo4j`."):
        G.drop(failIfMissing=True)


def test_graph_creation_time(G: GraphV2) -> None:
    assert G.creation_time().year > 2000


def test_graph_modification_time(G: GraphV2) -> None:
    assert G.modification_time().year > 2000


def test_graph_str(G: GraphV2) -> None:
    assert str(G) == "GraphV2(name=g, node_count=4, relationship_count=4)"


def test_graph_repr(G: GraphV2) -> None:
    assert "'memory_usage'" in repr(G)
