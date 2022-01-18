from typing import Generator

import pytest

from gdsclient.graph.graph_object import Graph
from gdsclient.graph_data_science import GraphDataScience
from gdsclient.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(autouse=True)
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_query(
        """
        CREATE
        (a:Location {name: 'A'}),
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
    G = gds.graph.project(
        "g",
        "Location",
        {"ROAD": {"properties": "cost"}},
    )

    yield G

    G.drop()
    runner.run_query("MATCH (n) DETACH DELETE n")


def test_find_node_id(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    id = gds.find_node_id(["Location"], {"name": "A"})
    res = runner.run_query(f"MATCH(n) WHERE id(n) = {id} RETURN n")
    assert len(res) == 1
    assert res[0]["n"]["name"] == "A"

    id = gds.find_node_id(["Location"], {"name": 2})
    res = runner.run_query(f"MATCH(n) WHERE id(n) = {id} RETURN n")
    assert len(res) == 1
    assert res[0]["n"]["name"] == 2

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

    assert result[0]["totalCost"] == 160


def test_version(gds: GraphDataScience) -> None:
    result = gds.version()
    assert isinstance(result[0]["version"], str)
