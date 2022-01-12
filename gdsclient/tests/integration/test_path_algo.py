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
        (a:Location {name: 'A', latitude: 51.5308, longitude: -0.1238}),
        (b:Location {name: 'B', latitude: 51.5282, longitude: -0.1337}),
        (c:Location {name: 'C', latitude: 51.5392, longitude: -0.1426}),
        (d:Location {name: 'D', latitude: 51.5342, longitude: -0.1387}),
        (e:Location {name: 'G', latitude: 51.5507, longitude: -0.1401}),
        (f:Location {name: 2, latitude: 51.5308, longitude: -0.1238}),
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
        {"Location": {"properties": ["latitude", "longitude"]}},
        {"ROAD": {"properties": "cost"}},
    )

    yield G

    G.drop()
    runner.run_query("MATCH (n) DETACH DELETE n")


def test_match_dijkstra_source_target_stream(gds: GraphDataScience, G: Graph) -> None:
    source_match = {"labels": ["Location"], "properties": {"name": "A"}}
    target_match = {"labels": ["Location"], "properties": {"name": 2}}

    result = gds.shortestPath.dijkstra.stream.match(
        G,
        sourceNode=source_match,
        targetNode=target_match,
        relationshipWeightProperty="cost",
    )

    assert result[0]["totalCost"] == 160


def test_match_dijkstra_single_source_mutate(gds: GraphDataScience, G: Graph) -> None:
    source_match = {"labels": ["Location"], "properties": {"name": "A"}}

    result = gds.allShortestPaths.dijkstra.mutate.match(
        G,
        sourceNode=source_match,
        mutateRelationshipType="PATH",
    )

    assert result[0]["relationshipsWritten"] == 6
    assert G.relationship_properties("PATH") == ["totalCost"]


def test_match_AStar_write(
    runner: Neo4jQueryRunner, gds: GraphDataScience, G: Graph
) -> None:
    source_match = {"labels": ["Location"], "properties": {"name": "A"}}
    target_match = {"labels": ["Location"], "properties": {"name": 2}}

    result = gds.shortestPath.astar.write.match(
        G,
        sourceNode=source_match,
        targetNode=target_match,
        latitudeProperty="latitude",
        longitudeProperty="longitude",
        relationshipWeightProperty="cost",
        writeRelationshipType="PATH",
    )

    assert result[0]["relationshipsWritten"] == 1

    path_costs = runner.run_query(
        """
        MATCH(n)-[r:PATH]-(m)
        RETURN r.totalCost as totalCost
        """
    )
    assert path_costs[0]["totalCost"] == 160.0
