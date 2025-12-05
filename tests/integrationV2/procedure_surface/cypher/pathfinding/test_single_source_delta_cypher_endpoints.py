from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.single_source_delta_cypher_endpoints import (
    DeltaSteppingCypherEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph
from graphdatascience.tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_name


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
def delta_stepping_endpoints(query_runner: QueryRunner) -> Generator[DeltaSteppingCypherEndpoints, None, None]:
    yield DeltaSteppingCypherEndpoints(query_runner)


def test_delta_stepping_stream(delta_stepping_endpoints: DeltaSteppingCypherEndpoints, sample_graph: GraphV2) -> None:
    result_df = delta_stepping_endpoints.stream(
        G=sample_graph,
        source_node=find_node_by_name(delta_stepping_endpoints._query_runner, "A"),
        delta=3.0,
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"sourceNode", "targetNode", "totalCost", "nodeIds", "costs", "index"}
    assert len(result_df) == 5


def test_delta_stepping_stats(delta_stepping_endpoints: DeltaSteppingCypherEndpoints, sample_graph: GraphV2) -> None:
    result = delta_stepping_endpoints.stats(
        G=sample_graph,
        source_node=find_node_by_name(delta_stepping_endpoints._query_runner, "A"),
        delta=3.0,
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert "sourceNode" in result.configuration


def test_delta_stepping_mutate(delta_stepping_endpoints: DeltaSteppingCypherEndpoints, sample_graph: GraphV2) -> None:
    result = delta_stepping_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="PATH",
        source_node=find_node_by_name(delta_stepping_endpoints._query_runner, "A"),
        delta=3.0,
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == 5
    assert "sourceNode" in result.configuration


def test_delta_stepping_write(delta_stepping_endpoints: DeltaSteppingCypherEndpoints, sample_graph: GraphV2) -> None:
    result = delta_stepping_endpoints.write(
        G=sample_graph,
        write_relationship_type="PATH",
        source_node=find_node_by_name(delta_stepping_endpoints._query_runner, "A"),
        delta=3.0,
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == 5
    assert "sourceNode" in result.configuration


def test_delta_stepping_estimate(delta_stepping_endpoints: DeltaSteppingCypherEndpoints, sample_graph: GraphV2) -> None:
    result = delta_stepping_endpoints.estimate(
        sample_graph,
        source_node=find_node_by_name(delta_stepping_endpoints._query_runner, "A"),
        delta=3.0,
        relationship_weight_property="cost",
    )

    assert result.node_count == 5
    assert result.relationship_count == 8
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
