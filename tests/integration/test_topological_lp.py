from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner import QueryType
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion


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
        """,
        QueryType.USER_ACTION,
    )

    yield

    runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


@pytest.fixture
def node_a(gds: GraphDataScience) -> int:
    return gds.find_node_id(["Node"], {"x": 3})


@pytest.fixture
def node_c(gds: GraphDataScience) -> int:
    return gds.find_node_id(["Node"], {"x": 7})


@pytest.fixture
def node_b(gds: GraphDataScience) -> int:
    return gds.find_node_id(["Node"], {"x": 5})


@pytest.mark.filterwarnings("ignore: .*gds.alpha.linkprediction.adamicAdar.*")
def test_alpha_adamicAdar(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.adamicAdar(node_a, node_c)
    assert score == pytest.approx(0.72, 0.01)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 24, 0))
def test_adamicAdar(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.linkprediction.adamicAdar(node_a, node_c)
    assert score == pytest.approx(0.72, 0.01)


@pytest.mark.filterwarnings("ignore: .*gds.alpha.linkprediction.commonNeighbors.*")
def test_alpha_commonNeighbors(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.commonNeighbors(node_a, node_c)
    assert score == 1


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 24, 0))
def test_commonNeighbors(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.linkprediction.commonNeighbors(node_a, node_c)
    assert score == 1


@pytest.mark.filterwarnings("ignore: .*gds.alpha.linkprediction.preferentialAttachment.*")
def test_alpha_preferentialAttachment(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.preferentialAttachment(node_a, node_c)
    assert score == 16


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 24, 0))
def test_preferentialAttachment(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.linkprediction.preferentialAttachment(node_a, node_c)
    assert score == 16


@pytest.mark.filterwarnings("ignore: .*gds.alpha.linkprediction.resourceAllocation.*")
def test_alpha_resourceAllocation(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.resourceAllocation(node_a, node_c, direction="BOTH")
    assert score == pytest.approx(0.25, 0.01)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 24, 0))
def test_resourceAllocation(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.linkprediction.resourceAllocation(node_a, node_c, direction="BOTH")
    assert score == pytest.approx(0.25, 0.01)


@pytest.mark.filterwarnings("ignore: .*gds.alpha.linkprediction.sameCommunity.*")
def test_alpha_sameCommunity(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.sameCommunity(node_a, node_c, communityProperty="y")
    assert score == 0


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 24, 0))
def test_sameCommunity(node_a: int, node_c: int, node_b: int, gds: GraphDataScience) -> None:
    assert gds.linkprediction.sameCommunity(node_a, node_c, communityProperty="y") == 0
    assert gds.linkprediction.sameCommunity(node_a, node_b, communityProperty="y") == 1


@pytest.mark.filterwarnings("ignore: .*gds.alpha.linkprediction.totalNeighbors.*")
def test_alpha_totalNeighbors(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.alpha.linkprediction.totalNeighbors(node_a, node_c)
    assert score == 3


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 24, 0))
def test_totalNeighbors(node_a: int, node_c: int, gds: GraphDataScience) -> None:
    score = gds.linkprediction.totalNeighbors(node_a, node_c)
    assert score == 3
