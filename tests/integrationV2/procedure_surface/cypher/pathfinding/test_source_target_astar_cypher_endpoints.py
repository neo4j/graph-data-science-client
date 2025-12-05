from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.source_target_astar_cypher_endpoints import (
    AStarCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_name


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {name: 'A', latitude: 55.6761, longitude: 12.5683}),
    (b: Node {name: 'B', latitude: 55.6869, longitude: 12.5985}),
    (c: Node {name: 'C', latitude: 55.6889, longitude: 12.5872}),
    (d: Node {name: 'D', latitude: 55.6867, longitude: 12.5760}),
    (e: Node {name: 'E', latitude: 55.6842, longitude: 12.5681}),
    (a)-[:ROAD {cost: 50}]->(b),
    (a)-[:ROAD {cost: 50}]->(c),
    (a)-[:ROAD {cost: 100}]->(d),
    (b)-[:ROAD {cost: 40}]->(d),
    (c)-[:ROAD {cost: 40}]->(d),
    (c)-[:ROAD {cost: 80}]->(e),
    (d)-[:ROAD {cost: 30}]->(e),
    (d)-[:ROAD {cost: 80}]->(a)
    """

    projection_query = """
        MATCH (source)-[r]->(target)
        WITH gds.graph.project('g', source, target, {
            sourceNodeProperties: {longitude: source.longitude, latitude: source.latitude},
            targetNodeProperties: {longitude: target.longitude, latitude: target.latitude},
            relationshipProperties: properties(r)
        }) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def astar_endpoints(query_runner: QueryRunner) -> Generator[AStarCypherEndpoints, None, None]:
    yield AStarCypherEndpoints(query_runner)


def test_astar_stream(astar_endpoints: AStarCypherEndpoints, sample_graph: GraphV2) -> None:
    result_df = astar_endpoints.stream(
        G=sample_graph,
        source_node=find_node_by_name(astar_endpoints._query_runner, "A"),
        target_node=find_node_by_name(astar_endpoints._query_runner, "E"),
        latitude_property="latitude",
        longitude_property="longitude",
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"sourceNode", "targetNode", "totalCost", "nodeIds", "costs", "index"}
    assert len(result_df) == 1


def test_astar_mutate(astar_endpoints: AStarCypherEndpoints, sample_graph: GraphV2) -> None:
    result = astar_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="PATH",
        source_node=find_node_by_name(astar_endpoints._query_runner, "A"),
        target_node=find_node_by_name(astar_endpoints._query_runner, "E"),
        latitude_property="latitude",
        longitude_property="longitude",
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == 1
    assert "sourceNode" in result.configuration


def test_astar_write(astar_endpoints: AStarCypherEndpoints, sample_graph: GraphV2) -> None:
    result = astar_endpoints.write(
        G=sample_graph,
        write_relationship_type="PATH",
        source_node=find_node_by_name(astar_endpoints._query_runner, "A"),
        target_node=find_node_by_name(astar_endpoints._query_runner, "E"),
        latitude_property="latitude",
        longitude_property="longitude",
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == 1
    assert "sourceNode" in result.configuration


def test_astar_estimate(astar_endpoints: AStarCypherEndpoints, sample_graph: GraphV2) -> None:
    result = astar_endpoints.estimate(
        sample_graph,
        source_node=find_node_by_name(astar_endpoints._query_runner, "A"),
        target_node=find_node_by_name(astar_endpoints._query_runner, "E"),
        latitude_property="latitude",
        longitude_property="longitude",
        relationship_weight_property="cost",
    )

    assert result.node_count == 5
    assert result.relationship_count == 8
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
