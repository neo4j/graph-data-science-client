from typing import Generator

import pytest

from graphdatascience import QueryRunner, ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints


@pytest.fixture(scope="package")
def gds(arrow_client: AuthenticatedArrowClient, db_query_runner: QueryRunner) -> AuraGraphDataScience:
    return AuraGraphDataScience(
        query_runner=db_query_runner,
        delete_fn=lambda: True,
        gds_version=ServerVersion.from_string("1.2.3"),
        v2_endpoints=SessionV2Endpoints(arrow_client, db_query_runner),
    )


@pytest.fixture(autouse=True, scope="class")
def setup_db(db_query_runner: QueryRunner) -> Generator[None, None, None]:
    db_query_runner.run_cypher("""
        CREATE (n)-[:REL]->(m)
    """)

    yield

    db_query_runner.run_cypher("""
        MATCH (n) DETACH DELETE n
    """)


@pytest.mark.db_integration
def test_walking_skeleton(gds: AuraGraphDataScience) -> None:
    g_and_result = gds.v2.graph.project("g", "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")

    G = g_and_result.graph
    project_result = g_and_result.result
    assert project_result.node_count == 2

    wcc_mutate_result = gds.v2.wcc.mutate(G, mutate_property="wcc")

    assert wcc_mutate_result.component_count == 1

    pr_result = gds.v2.page_rank.stream(G, damping_factor=0.5)
    assert len(pr_result) == 2
    assert {"nodeId", "score"} == set(pr_result.columns.to_list())

    fastrp_result = gds.v2.fast_rp.write(G, write_property="fastRP", embedding_dimension=2)
    assert fastrp_result.node_properties_written == 2
    assert gds.run_cypher("MATCH (n) WHERE n.fastRP IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 2

    drop_result = gds.v2.graph.node_properties.drop(G, node_properties=["wcc"])
    assert drop_result.properties_removed == 2
