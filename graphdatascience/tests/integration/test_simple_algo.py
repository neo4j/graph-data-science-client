from typing import Generator

from pytest import fixture

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

GRAPH_NAME = "g"


@fixture(autouse=True)
def run_around_tests(runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    # Runs before each test
    runner.run_query(
        """
        CREATE
        (a: Node),
        (b: Node),
        (c: Node),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}')")


def test_pageRank_mutate(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.pageRank.mutate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)
    assert result["nodePropertiesWritten"] == 3

    props = runner.run_query(
        f"""
        CALL gds.graph.list('{GRAPH_NAME}')
        YIELD schema
        RETURN schema.nodes as properties
        """
    )[0]["properties"]["__ALL__"].keys()
    assert list(props) == ["rank"]


def test_pageRank_mutate_estimate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.pageRank.mutate.estimate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert result["requiredMemory"]


def test_wcc_stats(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.wcc.stats(G)

    assert result["componentCount"] == 1


def test_wcc_stats_estimate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.wcc.stats.estimate(G)

    assert result["requiredMemory"]


def test_nodeSimilarity_stream(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.nodeSimilarity.stream(G, similarityCutoff=0)

    assert len(result) == 2
    assert result[0]["similarity"] == 0.5


def test_nodeSimilarity_stream_estimate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.nodeSimilarity.stream.estimate(G, similarityCutoff=0)

    assert result["requiredMemory"]


def test_fastRP_write(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.fastRP.write(G, writeProperty="embedding", embeddingDimension=4, randomSeed=42)
    assert result["nodePropertiesWritten"] == 3

    embeddings = runner.run_query(
        """
        MATCH(n:Node)
        RETURN n.embedding as embedding
        """
    )
    assert len(embeddings) == 3
    assert len(embeddings[0]["embedding"]) == 4
    assert len(embeddings[1]["embedding"]) == 4
    assert len(embeddings[2]["embedding"]) == 4


def test_fastRP_write_estimate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.fastRP.write.estimate(G, writeProperty="embedding", embeddingDimension=4, randomSeed=42)

    assert result["requiredMemory"]
