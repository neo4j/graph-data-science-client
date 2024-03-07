from typing import Generator

from pytest import fixture

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

GRAPH_NAME = "g"


@fixture(autouse=True)
def run_around_tests(gds: GraphDataScience) -> Generator[None, None, None]:
    # Runs before each test
    gds.run_cypher(
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
    gds.run_cypher("MATCH (n) DETACH DELETE n")
    gds.graph.drop(GRAPH_NAME)


def test_pageRank_mutate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.pageRank.mutate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)
    assert result["nodePropertiesWritten"] == 3
    assert G.node_properties("__ALL__") == ["rank"]


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
    assert result["similarity"][0] == 0.5


def test_fastRP_stream_exercise_logging(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    # Run for long enough that logging code is exercised.
    gds.fastRP.stream(G, iterationWeights=list(range(10_000)), embeddingDimension=100)


def test_nodeSimilarity_stream_estimate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.nodeSimilarity.stream.estimate(
        G,
        similarityCutoff=0,
    )

    assert result["requiredMemory"]


def test_fastRP_write(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.fastRP.write(G, writeProperty="embedding", embeddingDimension=4, randomSeed=42)
    assert result["nodePropertiesWritten"] == 3

    embeddings = runner.run_cypher(
        """
        MATCH(n:Node)
        RETURN n.embedding as embedding
        """
    )
    assert len(embeddings) == 3
    assert len(embeddings["embedding"][0]) == 4
    assert len(embeddings["embedding"][1]) == 4
    assert len(embeddings["embedding"][2]) == 4


def test_fastRP_write_estimate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.fastRP.write.estimate(G, writeProperty="embedding", embeddingDimension=4, randomSeed=42)

    assert result["requiredMemory"]


def test_nodeSimilarity_stream_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.nodeSimilarity.stream(G, similarityCutoff=0, stream_with_arrow=True)

    assert len(result) == 2
    assert result["propertyValue"][0] == 0.5


def test_fastRP_stream_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.fastRP.stream(G, stream_with_arrow=True, embeddingDimension=32)

    assert len(result) == 3
    assert len(result["propertyValue"][0]) == 32
