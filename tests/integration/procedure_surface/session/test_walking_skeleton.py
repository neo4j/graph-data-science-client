from typing import Generator
from unittest import mock

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner import QueryType
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager


@pytest.fixture(scope="package")
def gds(arrow_client: AuthenticatedArrowClient, db_query_runner: Neo4jQueryRunner) -> AuraGraphDataScience:
    return AuraGraphDataScience(
        arrow_client,
        db_query_runner,
        session_lifecycle_manager=mock.Mock(spec=SessionLifecycleManager),
    )


@pytest.fixture(autouse=True, scope="class")
def setup_db(db_query_runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    db_query_runner.run_cypher(
        """
        CREATE (n)-[:REL]->(m)
    """,
        query_type=QueryType.USER_ACTION,
    )

    yield

    db_query_runner.run_cypher(
        """
        MATCH (n) DETACH DELETE n
    """,
        query_type=QueryType.USER_ACTION,
    )


@pytest.mark.db_integration
def test_walking_skeleton(gds: AuraGraphDataScience) -> None:
    g_and_result = gds.graph.project("g", "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")

    G = g_and_result.graph
    project_result = g_and_result.result
    assert project_result.node_count == 2

    wcc_mutate_result = gds.wcc.mutate(G, mutate_property="wcc")

    assert wcc_mutate_result.component_count == 1

    pr_result = gds.page_rank.stream(G, damping_factor=0.5)
    assert len(pr_result) == 2
    assert {"nodeId", "score"} == set(pr_result.columns.to_list())

    fastrp_result = gds.fast_rp.write(G, write_property="fastRP", embedding_dimension=2)
    assert fastrp_result.node_properties_written == 2
    assert gds.run_cypher("MATCH (n) WHERE n.fastRP IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 2

    drop_result = gds.graph.node_properties.drop(G, node_properties=["wcc"])
    assert drop_result.properties_removed == 2
