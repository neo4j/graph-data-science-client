from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_endpoints import AStarWriteResult
from graphdatascience.procedure_surface.arrow.pathfinding.source_target_astar_arrow_endpoints import AStarArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_id

graph = """
        CREATE
            (a: Node {id: 0, latitude: 55.6761, longitude: 12.5683}),
            (b: Node {id: 1, latitude: 55.6869, longitude: 12.5985}),
            (c: Node {id: 2, latitude: 55.6889, longitude: 12.5872}),
            (d: Node {id: 3, latitude: 55.6867, longitude: 12.5760}),
            (e: Node {id: 4, latitude: 55.6842, longitude: 12.5681}),
            (a)-[:ROAD {cost: 50}]->(b),
            (a)-[:ROAD {cost: 50}]->(c),
            (a)-[:ROAD {cost: 100}]->(d),
            (b)-[:ROAD {cost: 40}]->(d),
            (c)-[:ROAD {cost: 40}]->(d),
            (c)-[:ROAD {cost: 80}]->(e),
            (d)-[:ROAD {cost: 30}]->(e),
            (d)-[:ROAD {cost: 80}]->(a)
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
                    MATCH (source)-[r]->(target)
                    WITH gds.graph.project.remote(source, target, {sourceNodeProperties: properties(source), targetNodeProperties: properties(target), relationshipProperties: properties(r)}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def astar_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[AStarArrowEndpoints, None, None]:
    yield AStarArrowEndpoints(arrow_client)


def test_astar_stream(astar_endpoints: AStarArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = astar_endpoints.stream(
        G=sample_graph,
        source_node=0,
        target_node=4,
        latitude_property="latitude",
        longitude_property="longitude",
        relationship_weight_property="cost",
    )

    assert len(result_df) > 0
    assert "sourceNode" in result_df.columns
    assert "targetNode" in result_df.columns
    assert "totalCost" in result_df.columns


def test_astar_mutate(astar_endpoints: AStarArrowEndpoints, sample_graph: GraphV2) -> None:
    result = astar_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="PATH",
        source_node=0,
        target_node=4,
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


@pytest.mark.db_integration
def test_astar_write(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    db_graph: GraphV2,
) -> None:
    endpoints_with_writeback = AStarArrowEndpoints(
        arrow_client=arrow_client,
        write_back_client=RemoteWriteBackClient(arrow_client, query_runner),
    )

    result = endpoints_with_writeback.write(
        G=db_graph,
        write_relationship_type="PATH",
        source_node=find_node_by_id(query_runner, 0),
        target_node=4,
        latitude_property="latitude",
        longitude_property="longitude",
        relationship_weight_property="cost",
    )

    assert isinstance(result, AStarWriteResult)
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == 1
    assert "sourceNode" in result.configuration


def test_astar_estimate(astar_endpoints: AStarArrowEndpoints, sample_graph: GraphV2) -> None:
    result = astar_endpoints.estimate(
        sample_graph,
        source_node=0,
        target_node=4,
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
