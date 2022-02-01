from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(autouse=True)
def run_around_tests(runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    runner.run_query(
        """
        CREATE
        (a:Location {name: 'A', population: 1337}),
        (b:Location {name: 'B'}),
        (c:Location {name: 'C'}),
        (d:Location {name: 'D'}),
        (e:Location {name: 'G'}),
        (f:Location {name: 2}),
        (a)-[:ROAD {cost: 50}]->(b),
        (a)-[:ROAD {cost: 50}]->(c),
        (a)-[:ROAD {cost: 100}]->(d),
        (b)-[:ROAD {cost: 40}]->(d),
        (c)-[:ROAD {cost: 40}]->(d),
        (c)-[:ROAD {cost: 80}]->(e),
        (d)-[:ROAD {cost: 30}]->(e),
        (d)-[:ROAD {cost: 80}]->(f),
        (e)-[:ROAD {cost: 40}]->(f)
        """
    )

    yield

    runner.run_query("MATCH (n) DETACH DELETE n")


def test_similarity_jaccard(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.jaccard([1, 2, 3], [1, 2, 4, 5])
    assert result == pytest.approx(0.4, 0.01)


def test_similarity_cosine(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.cosine([3, 8, 7, 5, 2, 9], [10, 8, 6, 6, 4, 5])
    assert result == pytest.approx(0.86, 0.01)


def test_similarity_pearson(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.pearson([420, 13.37], [13.37, 42.0])
    assert result == pytest.approx(-1.0, 0.01)


def test_similarity_euclideanDistance(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.euclideanDistance(
        [3, 8, 7, 5, 2, 9], [10, 8, 6, 6, 4, 5]
    )
    assert result == pytest.approx(8.42, 0.01)


def test_similarity_overlap(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.overlap([1, 2, 3], [1, 2, 4, 5])
    assert result == pytest.approx(0.666, 0.01)


def test_similarity_cosine_stats(gds: GraphDataScience) -> None:
    node1 = {"item": 1, "weights": [42.0, 13.37]}
    node2 = {"item": 2, "weights": [13.37, 42]}

    result = gds.alpha.similarity.cosine.stats(data=[node1, node2])
    assert result["nodes"] == 2


def test_similarity_pearson_stream(gds: GraphDataScience) -> None:
    node1 = {"item": 1, "weights": [42.0, 13.37]}
    node2 = {"item": 2, "weights": [13.37, 42]}

    result = gds.alpha.similarity.pearson.stream(data=[node1, node2])
    assert result["similarity"] == pytest.approx(-1.0, 0.01)


def test_similarity_euclideanDistance_write(gds: GraphDataScience) -> None:
    node_id1 = gds.find_node_id(["Location"], {"name": "A"})
    node_id2 = gds.find_node_id(["Location"], {"name": 2})
    node1 = {"item": node_id1, "weights": [42.0, 13.37]}
    node2 = {"item": node_id2, "weights": [13.37, 42]}

    result = gds.alpha.similarity.euclidean.write(data=[node1, node2])
    assert result["nodes"] == 2


def test_similarity_ann_stream(gds: GraphDataScience) -> None:
    node1 = {"item": 1, "weights": [42.0, 13.37]}
    node2 = {"item": 2, "weights": [13.37, 42]}

    result = gds.alpha.ml.ann.stream(algorithm="euclidean", data=[node1, node2])
    assert result["similarity"] == pytest.approx(1639.353, 0.01)
