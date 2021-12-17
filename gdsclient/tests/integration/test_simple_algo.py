from pytest import fixture
from gdsclient import GraphDataScience, Neo4jQueryRunner
from neo4j import GraphDatabase


URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
DRIVER = GraphDatabase.driver(URI)
RUNNER = Neo4jQueryRunner(DRIVER)
gds = GraphDataScience(RUNNER)


@fixture(autouse=True)
def run_around_tests():
    # Runs before each test
    with DRIVER.session() as session:
        session.run(
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

    yield

    # Runs after each test
    with DRIVER.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(f"CALL gds.graph.drop('{GRAPH_NAME}')")


def test_pageRank_mutate():
    graph = gds.graph.create(GRAPH_NAME, "*", "*")

    gds.pageRank.mutate(graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    props = RUNNER.run_query(
        f"""
        CALL gds.graph.list('{GRAPH_NAME}')
        YIELD schema
        RETURN schema.nodes as properties
        """
    )[0]["properties"]["__ALL__"].keys()
    assert list(props) == ["rank"]


def test_wcc_stats():
    graph = gds.graph.create(GRAPH_NAME, "*", "*")

    result = gds.wcc.stats(graph)

    assert result[0]["componentCount"] == 1


def test_nodeSimilarity_stream():
    graph = gds.graph.create(GRAPH_NAME, "*", "*")

    result = gds.nodeSimilarity.stream(graph, similarityCutoff=0)

    assert len(result) == 2
    assert result[0]["similarity"] == 0.5


def test_fastRP_write():
    graph = gds.graph.create(GRAPH_NAME, "*", "*")

    gds.fastRP.write(
        graph, writeProperty="embedding", embeddingDimension=4, randomSeed=42
    )

    embeddings = RUNNER.run_query(
        """
        MATCH(n:Node)
        RETURN n.embedding as embedding
        """
    )
    assert len(embeddings) == 3
    assert embeddings[0]["embedding"][0] != 0
