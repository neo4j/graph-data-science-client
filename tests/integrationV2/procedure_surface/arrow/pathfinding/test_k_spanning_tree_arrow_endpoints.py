from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pathfinding.k_spanning_tree_arrow_endpoints import (
    KSpanningTreeArrowEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
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


@pytest.mark.db_integration
def test_k_spanning_tree_write(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    k_spanning_tree_endpoints = KSpanningTreeArrowEndpoints(
        arrow_client, write_back_client=RemoteWriteBackClient(arrow_client, query_runner)
    )
    result = k_spanning_tree_endpoints.write(
        G=db_graph,
        k=3,
        write_property="weight",
        source_node=0,
        relationship_weight_property="cost",
    )

    assert result.effective_node_count == 3
    assert result.write_millis >= 0
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
