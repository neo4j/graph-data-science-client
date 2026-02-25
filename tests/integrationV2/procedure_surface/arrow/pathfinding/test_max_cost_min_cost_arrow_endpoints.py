from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.max_flow_min_cost_endpoints import (
    MaxFlowMinCostMutateResult,
    MaxFlowMinCostStatsResult,
    MaxFlowMinCostWriteResult,
)
from graphdatascience.procedure_surface.arrow.pathfinding.max_flow_min_cost_arrow_endpoints import (
    MaxFlowMinCostArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_id

graph = """
        CREATE
              (a:Node {id: 0, capacity: 1.0, cost: 1.0}),
              (b:Node {id: 1, capacity: 1.0, cost: 1.0}),
              (c:Node {id: 2, capacity: 1.0, cost: 1.0}),
              (d:Node {id: 3, capacity: 1.0, cost: 1.0}),
              (e:Node {id: 4, capacity: 1.0, cost: 1.0}),
              (f:Node {id: 5, capacity: 1.0, cost: 1.0}),
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
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)-[r]->(m)
                    WITH gds.graph.project.remote(n, m, {
                        relationshipProperties: properties(r),
                        sourceNodeProperties: properties(n),
                        targetNodeProperties: properties(m)
                    }) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def min_cost_endpoints(arrow_client: AuthenticatedArrowClient) -> MaxFlowMinCostArrowEndpoints:
    return MaxFlowMinCostArrowEndpoints(arrow_client)


def test_min_cost_stats(min_cost_endpoints: MaxFlowMinCostArrowEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.stats(
        sample_graph,
        source_nodes=[0],
        target_nodes=[5],
        capacity_property="capacity",
        cost_property="cost",
        alpha=5,
    )

    assert isinstance(result, MaxFlowMinCostStatsResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.configuration["alpha"] == 5


def test_min_cost_stream(min_cost_endpoints: MaxFlowMinCostArrowEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.stream(
        sample_graph, capacity_property="capacity", cost_property="cost", source_nodes=[0], target_nodes=[5]
    )

    assert set(result.columns) == {"source", "target", "flow"}
    assert len(result) > 0


def test_min_cost_mutate(min_cost_endpoints: MaxFlowMinCostArrowEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.mutate(
        sample_graph,
        [0],
        [5],
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


@pytest.mark.db_integration
def test_min_cost_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    endpoints = MaxFlowMinCostArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(
        db_graph,
        [find_node_by_id(query_runner, 0)],
        [find_node_by_id(query_runner, 5)],
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


def test_min_cost_estimate(min_cost_endpoints: MaxFlowMinCostArrowEndpoints, sample_graph: GraphV2) -> None:
    result = min_cost_endpoints.estimate(
        G=sample_graph, capacity_property="capacity", cost_property="cost", source_nodes=[0], target_nodes=[5]
    )

    assert result.node_count == 6
    assert result.relationship_count == 8
    assert "Bytes" in result.required_memory
    assert result.tree_view is not None
    assert isinstance(result.map_view, dict)
    assert result.bytes_min >= 0
    assert result.bytes_max >= 0
    assert result.heap_percentage_min >= 0
    assert result.heap_percentage_max >= 0
