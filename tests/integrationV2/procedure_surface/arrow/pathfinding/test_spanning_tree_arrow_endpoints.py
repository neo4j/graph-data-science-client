from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pathfinding.spanning_tree_arrow_endpoints import (
    SpanningTreeArrowEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

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
    with create_graph(arrow_client, "g", graph, undirected=("LINK", "LINK_UNDIRECTED")) as G:
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
                    WITH gds.graph.project.remote(source, target, {relationshipProperties: properties(r)}) as g
                    RETURN g
                """,
        undirected_relationship_types=["*"],
    ) as g:
        yield g


@pytest.fixture
def spanning_tree_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[SpanningTreeArrowEndpoints, None, None]:
    yield SpanningTreeArrowEndpoints(arrow_client)


def test_spanning_tree_stream(spanning_tree_endpoints: SpanningTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = spanning_tree_endpoints.stream(
        G=sample_graph,
        source_node=0,
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"nodeId", "parentId", "weight"}
    assert len(result_df) == 5  # cypher has 6 as it includes the initial root node -> root node rel


def test_spanning_tree_stats(spanning_tree_endpoints: SpanningTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = spanning_tree_endpoints.stats(
        G=sample_graph,
        source_node=0,
        relationship_weight_property="cost",
    )

    assert result.total_weight == 5.0
    assert result.effective_node_count == 6
    assert result.compute_millis >= 0


def test_spanning_tree_mutate(spanning_tree_endpoints: SpanningTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = spanning_tree_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="TREE",
        mutate_property="weight",
        source_node=0,
        relationship_weight_property="cost",
    )

    assert result.total_weight == 5.0
    assert result.effective_node_count == 6
    assert result.relationships_written == 5
    assert result.mutate_millis >= 0


@pytest.mark.db_integration
def test_spanning_tree_write(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    spanning_tree_endpoints = SpanningTreeArrowEndpoints(
        arrow_client, write_back_client=RemoteWriteBackClient(arrow_client, query_runner)
    )
    result = spanning_tree_endpoints.write(
        G=db_graph,
        write_relationship_type="TREE",
        write_property="weight",
        source_node=0,
        relationship_weight_property="cost",
    )

    assert result.total_weight >= 0
    assert result.effective_node_count > 0
    assert result.relationships_written == 5
    assert result.write_millis >= 0


def test_spanning_tree_estimate(spanning_tree_endpoints: SpanningTreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = spanning_tree_endpoints.estimate(
        G=sample_graph,
        source_node=0,
        relationship_weight_property="cost",
    )

    assert result.bytes_min > 0
    assert result.bytes_max > 0
