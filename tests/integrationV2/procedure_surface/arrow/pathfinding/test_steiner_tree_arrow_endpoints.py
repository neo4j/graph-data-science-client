from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.steiner_tree_endpoints import SteinerTreeWriteResult
from graphdatascience.procedure_surface.arrow.pathfinding.steiner_tree_arrow_endpoints import SteinerTreeArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_id

graph = """
        CREATE
            (a: Node {id: 0}),
            (b: Node {id: 1}),
            (c: Node {id: 2}),
            (d: Node {id: 3}),
            (e: Node {id: 4}),
            (f: Node {id: 5}),
            (a)-[:LINK {cost: 1.0}]->(b),
            (a)-[:LINK {cost: 1.0}]->(c),
            (b)-[:LINK {cost: 1.0}]->(d),
            (c)-[:LINK {cost: 1.0}]->(e),
            (d)-[:LINK {cost: 1.0}]->(f),
            (e)-[:LINK {cost: 1.0}]->(f)
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
def steiner_tree_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[SteinerTreeArrowEndpoints, None, None]:
    yield SteinerTreeArrowEndpoints(arrow_client)


def test_steiner_tree_stream(steiner_tree_endpoints: SteinerTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = steiner_tree_endpoints.stream(
        G=sample_graph,
        source_node=0,
        target_nodes=[3, 4],
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"nodeId", "parentId", "weight"}
    assert (
        len(result_df) == 4
    )  # differs to Cypher implementation as there the initial source -> source relationship is included


def test_steiner_tree_stats(steiner_tree_endpoints: SteinerTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = steiner_tree_endpoints.stats(
        G=sample_graph,
        source_node=0,
        target_nodes=[3, 4],
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.effective_node_count == 5
    assert result.effective_target_nodes_count == 2
    assert result.total_weight == 4
    assert "sourceNode" in result.configuration


def test_steiner_tree_mutate(steiner_tree_endpoints: SteinerTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = steiner_tree_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="STEINER_TREE",
        mutate_property="weight",
        source_node=0,
        target_nodes=[3, 4],
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == 4
    assert result.effective_node_count == 5
    assert result.effective_target_nodes_count == 2
    assert result.total_weight == 4
    assert "sourceNode" in result.configuration


@pytest.mark.db_integration
def test_steiner_tree_write(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    db_graph: GraphV2,
) -> None:
    endpoints_with_writeback = SteinerTreeArrowEndpoints(
        arrow_client=arrow_client,
        write_back_client=RemoteWriteBackClient(arrow_client, query_runner),
    )

    result = endpoints_with_writeback.write(
        G=db_graph,
        write_relationship_type="STEINER_TREE",
        write_property="weight",
        source_node=find_node_by_id(query_runner, 0),
        target_nodes=[find_node_by_id(query_runner, 3), find_node_by_id(query_runner, 4)],
        relationship_weight_property="cost",
    )

    assert isinstance(result, SteinerTreeWriteResult)
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == 4
    assert result.effective_node_count == 5
    assert result.effective_target_nodes_count == 2
    assert result.total_weight == 4
    assert "sourceNode" in result.configuration


def test_steiner_tree_estimate(steiner_tree_endpoints: SteinerTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = steiner_tree_endpoints.estimate(
        sample_graph,
        source_node=0,
        target_nodes=[3, 4],
        relationship_weight_property="cost",
    )

    assert result.node_count == 6
    assert result.relationship_count == 6
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
