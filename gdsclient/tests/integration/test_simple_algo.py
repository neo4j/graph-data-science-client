from neo4j import GraphDatabase
from pytest import fixture

from gdsclient import GraphDataScience, Neo4jQueryRunner

from . import AUTH, URI

GRAPH_NAME = "g"


def setup_module():
    global driver
    global runner
    global gds

    driver = GraphDatabase.driver(URI, auth=AUTH)
    runner = Neo4jQueryRunner(driver)
    gds = GraphDataScience(runner)


@fixture(autouse=True)
def run_around_tests():
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


def test_pageRank_mutate():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    gds.pageRank.mutate(graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    props = runner.run_query(
        f"""
        CALL gds.graph.list('{GRAPH_NAME}')
        YIELD schema
        RETURN schema.nodes as properties
        """
    )[0]["properties"]["__ALL__"].keys()
    assert list(props) == ["rank"]


def test_pageRank_mutate_estimate():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.pageRank.mutate.estimate(
        graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert result[0]["requiredMemory"]


def test_wcc_stats():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.wcc.stats(graph)

    assert result[0]["componentCount"] == 1


def test_wcc_stats_estimate():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.wcc.stats.estimate(graph)

    assert result[0]["requiredMemory"]


def test_nodeSimilarity_stream():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.nodeSimilarity.stream(graph, similarityCutoff=0)

    assert len(result) == 2
    assert result[0]["similarity"] == 0.5


def test_nodeSimilarity_stream_estimate():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.nodeSimilarity.stream.estimate(graph, similarityCutoff=0)

    assert result[0]["requiredMemory"]


def test_fastRP_write():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    gds.fastRP.write(
        graph, writeProperty="embedding", embeddingDimension=4, randomSeed=42
    )

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


def test_fastRP_write_estimate():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.fastRP.write.estimate(
        graph, writeProperty="embedding", embeddingDimension=4, randomSeed=42
    )

    assert result[0]["requiredMemory"]


def teardown_module():
    driver.close()
