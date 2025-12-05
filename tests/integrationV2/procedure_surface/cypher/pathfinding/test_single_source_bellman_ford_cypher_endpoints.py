from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.single_source_bellman_ford_cypher_endpoints import (
    BellmanFordCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_name


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {name: 'A'}),
    (b: Node {name: 'B'}),
    (c: Node {name: 'C'}),
    (d: Node {name: 'D'}),
    (e: Node {name: 'E'}),
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
def bellman_ford_endpoints(query_runner: QueryRunner) -> Generator[BellmanFordCypherEndpoints, None, None]:
    yield BellmanFordCypherEndpoints(query_runner)


def test_bellman_ford_stream(bellman_ford_endpoints: BellmanFordCypherEndpoints, sample_graph: GraphV2) -> None:
    result_df = bellman_ford_endpoints.stream(
        G=sample_graph,
        source_node=find_node_by_name(bellman_ford_endpoints._query_runner, "A"),
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {
        "sourceNode",
        "targetNode",
        "totalCost",
        "nodeIds",
        "costs",
        "index",
        "isNegativeCycle",
    }
    assert len(result_df) > 0


def test_bellman_ford_stats(bellman_ford_endpoints: BellmanFordCypherEndpoints, sample_graph: GraphV2) -> None:
    result = bellman_ford_endpoints.stats(
        G=sample_graph,
        source_node=find_node_by_name(bellman_ford_endpoints._query_runner, "A"),
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.contains_negative_cycle is False
    assert "sourceNode" in result.configuration


def test_bellman_ford_mutate(bellman_ford_endpoints: BellmanFordCypherEndpoints, sample_graph: GraphV2) -> None:
    result = bellman_ford_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="PATH",
        source_node=find_node_by_name(bellman_ford_endpoints._query_runner, "A"),
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written >= 1
    assert result.contains_negative_cycle is False
    assert "sourceNode" in result.configuration


def test_bellman_ford_write(bellman_ford_endpoints: BellmanFordCypherEndpoints, sample_graph: GraphV2) -> None:
    result = bellman_ford_endpoints.write(
        G=sample_graph,
        write_relationship_type="PATH",
        source_node=find_node_by_name(bellman_ford_endpoints._query_runner, "A"),
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written >= 1
    assert result.contains_negative_cycle is False
    assert "sourceNode" in result.configuration


def test_bellman_ford_estimate(bellman_ford_endpoints: BellmanFordCypherEndpoints, sample_graph: GraphV2) -> None:
    result = bellman_ford_endpoints.estimate(
        sample_graph,
        source_node=find_node_by_name(bellman_ford_endpoints._query_runner, "A"),
        relationship_weight_property="cost",
    )

    assert result.node_count == 5
    assert result.relationship_count == 8
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
