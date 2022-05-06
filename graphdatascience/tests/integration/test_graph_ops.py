from typing import Generator

import pandas
import pytest
from neo4j import DEFAULT_DATABASE

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.server_version.server_version import ServerVersion

GRAPH_NAME = "g"


@pytest.fixture(autouse=True)
def run_around_tests(runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    # Runs before each test
    runner.run_query(
        """
        CREATE
        (a: Node {x: 1}),
        (b: Node {x: 2}),
        (c: Node {x: 3}),
        (a)-[:REL {relX: 4}]->(b),
        (a)-[:REL {relX: 5}]->(c),
        (b)-[:REL {relX: 6}]->(c),
        (b)-[:REL2]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")


def test_project_graph_native(gds: GraphDataScience) -> None:
    G, result = gds.graph.project(GRAPH_NAME, "*", "*")
    assert G.name() == GRAPH_NAME
    assert result["graphName"] == GRAPH_NAME

    result = gds.graph.exists(G.name())
    assert result["exists"]


def test_project_graph_native_estimate(gds: GraphDataScience) -> None:
    result = gds.graph.project.estimate("*", "*")

    assert result["requiredMemory"]


def test_project_graph_cypher(gds: GraphDataScience) -> None:
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    G, result = gds.graph.project.cypher(GRAPH_NAME, node_query, relationship_query)

    assert G.name() == GRAPH_NAME
    assert result["graphName"] == GRAPH_NAME

    result = gds.graph.exists(G.name())
    assert result["exists"]


def test_project_graph_cypher_estimate(gds: GraphDataScience) -> None:
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    result = gds.graph.project.cypher.estimate(node_query, relationship_query)

    assert result["requiredMemory"]


def test_project_subgraph(runner: QueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    subG, result = gds.beta.graph.project.subgraph("s", from_G, "n.x > 1", "*", concurrency=2)

    assert subG.name() == "s"
    assert result["graphName"] == "s"

    result2 = gds.graph.list(subG)
    assert result2["nodeCount"][0] == 2

    runner.run_query(f"CALL gds.graph.drop('{subG.name()}')")


def test_graph_list(gds: GraphDataScience) -> None:
    result = gds.graph.list()
    assert len(result) == 0

    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")
    result = gds.graph.list()
    assert len(result) == 1

    result = gds.graph.list(G)
    assert result["graphName"][0] == GRAPH_NAME


def test_graph_exists(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.graph.exists(G.name())
    assert result["exists"]

    result = gds.graph.exists("bogusName")
    assert not result["exists"]


def test_graph_drop(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.graph.drop(G, True)
    assert result is not None
    assert result["graphName"] == GRAPH_NAME

    with pytest.raises(Exception):
        gds.graph.drop(G, True)


def test_graph_export(runner: QueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    MY_DB_NAME = "testdatabase"
    result = gds.graph.export(G, dbName=MY_DB_NAME, batchSize=10000)

    assert result["graphName"] == GRAPH_NAME
    assert result["dbName"] == MY_DB_NAME

    runner.run_query("CREATE DATABASE $dbName", {"dbName": MY_DB_NAME})
    runner.set_database(MY_DB_NAME)
    node_count = runner.run_query("MATCH (n) RETURN COUNT(n) AS c").squeeze()

    assert node_count == 3

    runner.run_query("DROP DATABASE $dbName", {"dbName": MY_DB_NAME})
    runner.set_database(DEFAULT_DATABASE)


def test_graph_get(gds: GraphDataScience) -> None:
    gds.graph.project(GRAPH_NAME, "*", "*")

    G = gds.graph.get(GRAPH_NAME)
    assert G.name() == GRAPH_NAME

    with pytest.raises(ValueError):
        gds.graph.get("bogusName")


def test_graph_streamNodeProperty(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.streamNodeProperty(G, "x", concurrency=2)
    assert {e for e in result["propertyValue"]} == {1, 2, 3}


def test_graph_streamNodeProperty_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds_without_arrow.graph.streamNodeProperty(G, "x", concurrency=2)
    assert {e for e in result["propertyValue"]} == {1, 2, 3}


def test_graph_streamNodeProperties(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.streamNodeProperties(G, ["x"], concurrency=2)
    assert {e for e in result["propertyValue"]} == {1, 2, 3}


def test_graph_streamRelationshipProperty(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": "relX"}})

    result = gds.graph.streamRelationshipProperty(G, "relX", concurrency=2)
    assert {e for e in result["propertyValue"]} == {4, 5, 6}


def test_graph_streamRelationshipProperty_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", {"REL": {"properties": "relX"}})

    result = gds_without_arrow.graph.streamRelationshipProperty(G, "relX", concurrency=2)
    assert {e for e in result["propertyValue"]} == {4, 5, 6}


def test_graph_streamRelationshipProperties(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": "relX"}})

    result = gds.graph.streamRelationshipProperties(G, ["relX"], concurrency=2)
    assert {e for e in result["propertyValue"]} == {4, 5, 6}


def test_graph_writeNodeProperties(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    gds.pageRank.mutate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    result = gds.graph.writeNodeProperties(G, ["rank"], concurrency=2)
    assert result["propertiesWritten"] == 3


def test_graph_writeRelationship(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    gds.nodeSimilarity.mutate(G, mutateRelationshipType="SIMILAR", mutateProperty="score", similarityCutoff=0)

    result = gds.graph.writeRelationship(G, "SIMILAR", "score", concurrency=2)
    assert result["relationshipsWritten"] == 2
    assert result["propertiesWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_removeNodeProperties_21(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.removeNodeProperties(G, ["x"], concurrency=2)
    assert result["propertiesRemoved"] == 3


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 1, 0))
def test_graph_removeNodeProperties_20(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.removeNodeProperties(G, ["x"], ["*"], concurrency=2)
    assert result["propertiesRemoved"] == 3


def test_graph_deleteRelationships(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", ["REL", "REL2"])

    result = gds.graph.deleteRelationships(G, "REL")
    assert result["deletedRelationships"] == 3


def test_graph_generate(gds: GraphDataScience) -> None:
    G, result = gds.beta.graph.generate(GRAPH_NAME, 12, 2)

    assert G.node_count() == 12
    assert result["generateMillis"] >= 0


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct(gds: GraphDataScience) -> None:
    nodes = pandas.DataFrame({"node_id": [0, 1, 2, 3]})
    relationships = pandas.DataFrame({"source_id": [0, 1, 2, 3], "target_id": [1, 2, 3, 0]})

    G = gds.alpha.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_multiple_dfs(gds: GraphDataScience) -> None:
    nodes = [pandas.DataFrame({"node_id": [0, 1]}), pandas.DataFrame({"node_id": [2, 3]})]
    relationships = pandas.DataFrame({"source_id": [0, 1, 2, 3], "target_id": [1, 2, 3, 0]})

    G = gds.alpha.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    nodes = pandas.DataFrame({"node_id": [0, 1, 2, 3]})
    relationships = pandas.DataFrame({"source_id": [0, 1, 2, 3], "target_id": [1, 2, 3, 0]})

    with pytest.raises(ValueError):
        gds_without_arrow.alpha.graph.construct("hello", nodes, relationships)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_abort(gds: GraphDataScience) -> None:
    bad_nodes = pandas.DataFrame({"bogus": [0, 1, 2, 3]})
    relationships = pandas.DataFrame({"source_id": [0, 1, 2, 3], "target_id": [1, 2, 3, 0]})

    with pytest.raises(Exception):
        gds.alpha.graph.construct("hello", bad_nodes, relationships)

    good_nodes = pandas.DataFrame({"node_id": [0, 1, 2, 3]})
    G = gds.alpha.graph.construct("hello", good_nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()
