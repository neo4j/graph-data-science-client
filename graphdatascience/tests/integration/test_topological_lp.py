from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(autouse=True)
def create_graph(runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    runner.run_cypher(
        """
        CREATE
        (a: Node {x: 3, y: 20}),
        (b: Node {x: 5, y: 20}),
        (c: Node {x: 7, y: 4}),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c),
        (b)-[:REL]->(a),
        (c)-[:REL]->(a),
        (c)-[:REL]->(b)
        """
    )

    yield

    runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def node1(gds: GraphDataScience) -> int:
    return gds.find_node_id(["Node"], {"x": 3})


@pytest.fixture
def node2(gds: GraphDataScience) -> int:
    return gds.find_node_id(["Node"], {"x": 7})


def test_adamicAdar(node1: int, node2: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.adamicAdar(node1, node2)
    assert score == pytest.approx(0.72, 0.01)


def test_commonNeighbors(node1: int, node2: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.commonNeighbors(node1, node2)
    assert score == 1


def test_preferentialAttachment(node1: int, node2: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.preferentialAttachment(node1, node2)
    assert score == 16


def test_resourceAllocation(node1: int, node2: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.resourceAllocation(node1, node2, direction="BOTH")
    assert score == pytest.approx(0.25, 0.01)


def test_sameCommunity(node1: int, node2: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.sameCommunity(node1, node2, communityProperty="y")
    assert score == 0


def test_totalNeighbors(node1: int, node2: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.totalNeighbors(node1, node2)
    assert score == 3
