from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.max_flow_endpoints import (
    MaxFlowMutateResult,
    MaxFlowStatsResult,
    MaxFlowWriteResult,
)
from graphdatascience.procedure_surface.arrow.pathfinding.max_flow_arrow_endpoints import MaxFlowArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_id

graph = """
        CREATE
              (a:Node {id: 0}),
              (b:Node {id: 1}),
              (c:Node {id: 2}),
              (d:Node {id: 3}),
              (e:Node {id: 4}),
              (f:Node {id: 5}),
              (a)-[:TYPE {capacity: 3.0}]->(b),
              (a)-[:TYPE {capacity: 1.0}]->(c),
              (b)-[:TYPE {capacity: 4.0}]->(d),
              (c)-[:TYPE {capacity: 2.0}]->(d),
              (b)-[:TYPE {capacity: 2.0}]->(e),
              (d)-[:TYPE {capacity: 1.0}]->(e),
              (d)-[:TYPE {capacity: 3.0}]->(f),
              (e)-[:TYPE {capacity: 2.0}]->(f)
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
                    WITH gds.graph.project.remote(n, m, {relationshipProperties: properties(r)}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def max_flow_endpoints(arrow_client: AuthenticatedArrowClient) -> MaxFlowArrowEndpoints:
    return MaxFlowArrowEndpoints(arrow_client)


def test_max_flow_stats(max_flow_endpoints: MaxFlowArrowEndpoints, sample_graph: GraphV2) -> None:
    result = max_flow_endpoints.stats(sample_graph, source_nodes=[0], target_nodes=[5], capacity_property="capacity")

    assert isinstance(result, MaxFlowStatsResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0


def test_max_flow_stream(max_flow_endpoints: MaxFlowArrowEndpoints, sample_graph: GraphV2) -> None:
    result = max_flow_endpoints.stream(sample_graph, capacity_property="capacity", source_nodes=[0], target_nodes=[5])

    assert set(result.columns) == {"source", "target", "flow"}
    assert len(result) == 7


def test_max_flow_mutate(max_flow_endpoints: MaxFlowArrowEndpoints, sample_graph: GraphV2) -> None:
    result = max_flow_endpoints.mutate(
        sample_graph,
        [0],
        [5],
        mutate_property="flow",
        mutate_relationship_type="FLOW",
        capacity_property="capacity",
    )

    assert isinstance(result, MaxFlowMutateResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == 7


@pytest.mark.db_integration
def test_max_flow_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    endpoints = MaxFlowArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(
        db_graph,
        [find_node_by_id(query_runner, 0)],
        [find_node_by_id(query_runner, 5)],
        write_property="flow",
        write_relationship_type="FLOW",
        capacity_property="capacity",
    )

    assert isinstance(result, MaxFlowWriteResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == 7


def test_max_flow_estimate(max_flow_endpoints: MaxFlowArrowEndpoints, sample_graph: GraphV2) -> None:
    result = max_flow_endpoints.estimate(
        G=sample_graph, capacity_property="capacity", source_nodes=[0], target_nodes=[5]
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
