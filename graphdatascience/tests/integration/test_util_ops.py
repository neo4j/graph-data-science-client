from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience


@pytest.fixture(autouse=True)
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_cypher(
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
    G, _ = gds.graph.project(
        "g",
        {"Location": {"properties": "population"}},
        {"ROAD": {"properties": "cost"}},
    )

    yield G

    G.drop()
    runner.run_cypher("MATCH (n) DETACH DELETE n")


def test_find_node_id(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    id = gds.find_node_id(["Location"], {"name": "A"})
    res = runner.run_cypher(f"MATCH(n) WHERE id(n) = {id} RETURN n")
    assert len(res) == 1
    assert res["n"][0]["name"] == "A"

    id = gds.find_node_id(["Location"], {"name": 2})
    res = runner.run_cypher(f"MATCH(n) WHERE id(n) = {id} RETURN n")
    assert len(res) == 1
    assert res["n"][0]["name"] == 2

    # No matches
    with pytest.raises(ValueError):
        gds.find_node_id(["Location"], {"name": "bogus"})

    # Too many matches
    with pytest.raises(ValueError):
        gds.find_node_id(["Location"])


def test_dijkstra_with_find_node_id(gds: GraphDataScience, G: Graph) -> None:
    source = gds.find_node_id(["Location"], {"name": "A"})
    target = gds.find_node_id(["Location"], {"name": 2})

    result = gds.shortestPath.dijkstra.stream(
        G,
        sourceNode=source,
        targetNode=target,
        relationshipWeightProperty="cost",
    )

    assert result["totalCost"][0] == 160


def test_version(gds: GraphDataScience) -> None:
    result = gds.version()
    assert isinstance(result, str)


def test_server_version(gds: GraphDataScience) -> None:
    result = gds.server_version()
    assert isinstance(result, ServerVersion)


def test_list(gds: GraphDataScience) -> None:
    result = gds.list()
    assert len(result) > 10


def test_util_asNode(gds: GraphDataScience) -> None:
    id = gds.find_node_id(["Location"], {"name": "A"})
    result = gds.util.asNode(id)
    assert result["name"] == "A"


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_util_as_node(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    id = gds_with_cloud_setup.find_node_id(["Location"], {"name": "A"})
    result = gds_with_cloud_setup.util.asNode(id)
    assert result["name"] == "A"


def test_util_asNodes(gds: GraphDataScience) -> None:
    ids = [
        gds.find_node_id(["Location"], {"name": "A"}),
        gds.find_node_id(["Location"], {"name": 2}),
    ]
    result = gds.util.asNodes(ids)
    assert len(result) == 2


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_util_as_nodes(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    ids = [
        gds_with_cloud_setup.find_node_id(["Location"], {"name": "A"}),
        gds_with_cloud_setup.find_node_id(["Location"], {"name": 2}),
    ]
    result = gds_with_cloud_setup.util.asNodes(ids)
    assert len(result) == 2


def test_util_nodeProperty(gds: GraphDataScience, G: Graph) -> None:
    id = gds.find_node_id(["Location"], {"name": "A"})
    result = gds.util.nodeProperty(G, id, "population")
    assert result == 1337


def test_ml_oneHotEncoding(gds: GraphDataScience) -> None:
    result = gds.alpha.ml.oneHotEncoding(["Chinese", "Indian", "Italian"], ["Italian"])
    assert result == [0, 0, 1]
