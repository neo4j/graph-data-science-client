from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pathfinding.prize_steiner_tree_arrow_endpoints import (
    PrizeSteinerTreeArrowEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node {id: 0, prize: 5.0}),
            (b: Node {id: 1, prize: 10.0}),
            (c: Node {id: 2, prize: 15.0}),
            (d: Node {id: 3, prize: 8.0}),
            (e: Node {id: 4, prize: 12.0}),
            (a)-[:LINK {cost: 1.0}]->(b),
            (b)-[:LINK {cost: 2.0}]->(c),
            (a)-[:LINK {cost: 3.0}]->(d),
            (d)-[:LINK {cost: 1.5}]->(e),
            (e)-[:LINK {cost: 2.5}]->(c)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(
        arrow_client,
        "g",
        graph,
        undirected=("LINK", "LINK_UNDIRECTED"),
    ) as G:
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
                    WITH gds.graph.project.remote(source, target, {
                        sourceNodeProperties: properties(source),
                        targetNodeProperties: properties(target),
                        relationshipProperties: properties(r)
                    }) as g
                    RETURN g
                """,
        undirected_relationship_types=["*"],
    ) as g:
        yield g


@pytest.fixture
def prize_steiner_tree_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[PrizeSteinerTreeArrowEndpoints, None, None]:
    yield PrizeSteinerTreeArrowEndpoints(arrow_client)


def test_prize_steiner_tree_stream(
    prize_steiner_tree_endpoints: PrizeSteinerTreeArrowEndpoints, sample_graph: GraphV2
) -> None:
    result_df = prize_steiner_tree_endpoints.stream(
        G=sample_graph,
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"nodeId", "parentId", "weight"}
    assert len(result_df) == 4


def test_prize_steiner_tree_stats(
    prize_steiner_tree_endpoints: PrizeSteinerTreeArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = prize_steiner_tree_endpoints.stats(
        G=sample_graph,
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.total_weight == 7.0
    assert result.sum_of_prizes == 50.0
    assert result.effective_node_count == 5
    assert result.compute_millis >= 0


def test_prize_steiner_tree_mutate(
    prize_steiner_tree_endpoints: PrizeSteinerTreeArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = prize_steiner_tree_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="TREE",
        mutate_property="weight",
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.total_weight == 7.0
    assert result.sum_of_prizes == 50.0
    assert result.effective_node_count == 5
    assert result.relationships_written == 4
    assert result.mutate_millis >= 0


@pytest.mark.db_integration
def test_prize_steiner_tree_write(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    prize_steiner_tree_endpoints = PrizeSteinerTreeArrowEndpoints(
        arrow_client=arrow_client, write_back_client=RemoteWriteBackClient(arrow_client, query_runner)
    )
    result = prize_steiner_tree_endpoints.write(
        G=db_graph,
        write_relationship_type="TREE",
        write_property="weight",
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.total_weight == 7.0
    assert result.sum_of_prizes == 50.0
    assert result.effective_node_count == 5
    assert result.relationships_written == 4
    assert result.write_millis >= 0


def test_prize_steiner_tree_estimate(
    prize_steiner_tree_endpoints: PrizeSteinerTreeArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = prize_steiner_tree_endpoints.estimate(
        G=sample_graph,
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.bytes_min > 0
    assert result.bytes_max > 0
