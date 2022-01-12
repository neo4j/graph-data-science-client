from typing import Generator

from pytest import fixture

from gdsclient.graph.graph_object import Graph
from gdsclient.graph_data_science import GraphDataScience
from gdsclient.query_runner.neo4j_query_runner import Neo4jQueryRunner


@fixture(autouse=True)
def G(runner: Neo4jQueryRunner, gds: GraphDataScience) -> Generator[Graph, None, None]:
    runner.run_query(
        """
        CREATE
        (a:Location {name: 'A'}),
        (b:Location {name: 'B'}),
        (c:Location {name: 'C'}),
        (d:Location {name: 'D'}),
        (e:Location {name: 'E'}),
        (f:Location {name: 'F'}),
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
    G = gds.graph.project("g", "Location", {"ROAD": {"properties": "cost"}})

    yield G

    G.drop()
    runner.run_query("MATCH (n) DETACH DELETE n")


def test_dijsktra_source_target_stream(gds: GraphDataScience, G: Graph) -> None:
    source_match = {"labels": ["Location"], "properties": {"name": "A"}}
    target_match = {"labels": ["Location"], "properties": {"name": "F"}}

    result = gds.shortestPath.dijkstra.stream.match(
        G,
        sourceNode=source_match,
        targetNode=target_match,
        relationshipWeightProperty="cost",
    )

    assert result[0]["totalCost"] == 160
