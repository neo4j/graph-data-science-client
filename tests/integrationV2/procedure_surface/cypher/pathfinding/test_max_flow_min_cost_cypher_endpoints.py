from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.max_flow_min_cost_endpoints import (
    MaxFlowMinCostMutateResult,
    MaxFlowMinCostStatsResult,
    MaxFlowMinCostWriteResult,
)
from graphdatascience.procedure_surface.cypher.pathfinding.max_flow_min_cost_cypher_endpoints import (
    MaxFlowMinCostCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_name

graph = """
        CREATE
              (a:Node {name: 'A', capacity: 1.0, cost: 1.0}),
              (b:Node {name: 'B', capacity: 1.0, cost: 1.0}),
              (c:Node {name: 'C', capacity: 1.0, cost: 1.0}),
              (d:Node {id: 3, name: 'D', capacity: 1.0, cost: 1.0}),
              (e:Node {name: 'E', capacity: 1.0, cost: 1.0}),
              (f:Node {name: 'F', capacity: 1.0, cost: 1.0}),
              (a)-[:TYPE {capacity: 3.0, cost: 1.0}]->(b),
              (a)-[:TYPE {capacity: 1.0, cost: 5.0}]->(c),
              (b)-[:TYPE {capacity: 4.0, cost: 1.0}]->(d),
              (c)-[:TYPE {capacity: 2.0, cost: 1.0}]->(d),
              (b)-[:TYPE {capacity: 2.0, cost: 2.0}]->(e),
              (d)-[:TYPE {capacity: 1.0, cost: 1.0}]->(e),
              (d)-[:TYPE {capacity: 3.0, cost: 1.0}]->(f),
              (e)-[:TYPE {capacity: 2.0, cost: 1.0}]->(f)
        """


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {
            sourceNodeProperties: n {.cost, .capacity},
            targetNodeProperties: m {.cost, .capacity},
            relationshipProperties: properties(r)
        }) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        graph,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def min_cost_endpoints(query_runner: QueryRunner) -> MaxFlowMinCostCypherEndpoints:
    return MaxFlowMinCostCypherEndpoints(query_runner)


def test_min_cost_stats(min_cost_endpoints: MaxFlowMinCostCypherEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.stats(
        sample_graph,
        source_nodes=[find_node_by_name(min_cost_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(min_cost_endpoints._query_runner, "F")],
        capacity_property="capacity",
        cost_property="cost",
        alpha=5,
    )

    assert isinstance(result, MaxFlowMinCostStatsResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.configuration["alpha"] == 5


def test_min_cost_stream(min_cost_endpoints: MaxFlowMinCostCypherEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.stream(
        sample_graph,
        capacity_property="capacity",
        cost_property="cost",
        source_nodes=[find_node_by_name(min_cost_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(min_cost_endpoints._query_runner, "F")],
    )

    assert "flow" in result.columns
    assert len(result) > 0


def test_min_cost_mutate(min_cost_endpoints: MaxFlowMinCostCypherEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.mutate(
        sample_graph,
        [find_node_by_name(min_cost_endpoints._query_runner, "A")],
        [find_node_by_name(min_cost_endpoints._query_runner, "F")],
        mutate_property="flow",
        mutate_relationship_type="FLOW",
        capacity_property="capacity",
        cost_property="cost",
    )

    assert isinstance(result, MaxFlowMinCostMutateResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written > 0


def test_min_cost_write(
    query_runner: QueryRunner, min_cost_endpoints: MaxFlowMinCostCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = min_cost_endpoints.write(
        sample_graph,
        [find_node_by_name(min_cost_endpoints._query_runner, "A")],
        [find_node_by_name(min_cost_endpoints._query_runner, "F")],
        write_property="flow",
        write_relationship_type="FLOW",
        capacity_property="capacity",
        cost_property="cost",
    )

    assert isinstance(result, MaxFlowMinCostWriteResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written > 0


def test_min_cost_estimate(min_cost_endpoints: MaxFlowMinCostCypherEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.estimate(
        G=sample_graph,
        source_nodes=[find_node_by_name(min_cost_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(min_cost_endpoints._query_runner, "F")],
        capacity_property="capacity",
        cost_property="cost",
    )

    assert result.node_count == 6
    assert result.relationship_count == 8
    assert result.bytes_min >= 0
