from typing import Generator

import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo


@pytest.fixture(scope="package")
def gds(neo4j_connection: DbmsConnectionInfo) -> GraphDataScience:
    return GraphDataScience(
        endpoint=neo4j_connection.uri,
        auth=(neo4j_connection.username, neo4j_connection.password),  # type: ignore
    )


@pytest.fixture(autouse=True, scope="class")
def setup_db(gds: GraphDataScience) -> Generator[None, None, None]:
    gds.run_cypher("""
        CREATE (n)-[:REL]->(m)
    """)

    yield

    gds.run_cypher("""
        MATCH (n) DETACH DELETE n
    """)


@pytest.mark.db_integration
def test_walking_skeleton(gds: GraphDataScience) -> None:
    g_and_result = gds.v2.graph.project("g", "*", "*")

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
