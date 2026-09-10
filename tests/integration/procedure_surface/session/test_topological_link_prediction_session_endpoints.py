import math
from typing import Generator
from unittest import mock

import pytest
from neo4j.graph import Node

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.procedure_surface.api.topological_link_prediction_endpoints import Direction
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager


@pytest.fixture(scope="module")
def gds(
    arrow_client: AuthenticatedArrowClient, db_query_runner: Neo4jQueryRunner
) -> Generator[AuraGraphDataScience, None, None]:
    # The aura-dev test image provides the Cypher surface but its kernel version
    # does not end in "aura", so it is not detected as hosted in Aura.
    with mock.patch(
        "graphdatascience.query_runner.db_environment_resolver.DbEnvironmentResolver.hosted_in_aura",
        return_value=True,
    ):
        yield AuraGraphDataScience(
            arrow_client,
            db_query_runner,
            session_lifecycle_manager=mock.Mock(spec=SessionLifecycleManager),
        )


@pytest.fixture
def sample_graph(db_query_runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    # a and b each connect to the same two neighbours c and d.
    create_query = """
        CREATE
        (a:Person {name: 'a', community: 1}),
        (b:Person {name: 'b', community: 1}),
        (c:Person {name: 'c', community: 2}),
        (d:Person {name: 'd', community: 2}),
        (a)-[:FRIEND]->(c),
        (a)-[:FRIEND]->(d),
        (b)-[:FRIEND]->(c),
        (b)-[:FRIEND]->(d)
    """
    try:
        db_query_runner.run_cypher(create_query, QueryType.USER_ACTION)
        yield
    finally:
        db_query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


@filter_id_func_deprecation_warning()
def _node_ids_by_name(db_query_runner: Neo4jQueryRunner) -> dict[str, int]:
    df = db_query_runner.run_cypher("MATCH (n:Person) RETURN id(n) AS id, n.name AS name", QueryType.USER_ACTION)
    return {row["name"]: int(row["id"]) for _, row in df.iterrows()}


def _nodes_by_name(db_query_runner: Neo4jQueryRunner) -> dict[str, Node]:
    df = db_query_runner.run_cypher("MATCH (n:Person) RETURN n AS node, n.name AS name", QueryType.USER_ACTION)
    return {row["name"]: row["node"] for _, row in df.iterrows()}


@pytest.mark.db_integration
def test_common_neighbors(gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner) -> None:
    ids = _node_ids_by_name(db_query_runner)

    assert gds.topological_link_prediction.common_neighbors(ids["a"], ids["b"]) == 2.0


@pytest.mark.db_integration
def test_total_neighbors(gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner) -> None:
    ids = _node_ids_by_name(db_query_runner)

    assert gds.topological_link_prediction.total_neighbors(ids["a"], ids["b"]) == 2.0


@pytest.mark.db_integration
def test_preferential_attachment(
    gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner
) -> None:
    ids = _node_ids_by_name(db_query_runner)

    assert gds.topological_link_prediction.preferential_attachment(ids["a"], ids["b"]) == 4.0


@pytest.mark.db_integration
def test_resource_allocation(gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner) -> None:
    ids = _node_ids_by_name(db_query_runner)

    # common neighbours c and d each have degree 2 -> 1/2 + 1/2
    assert gds.topological_link_prediction.resource_allocation(ids["a"], ids["b"]) == pytest.approx(1.0)


@pytest.mark.db_integration
def test_adamic_adar(gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner) -> None:
    ids = _node_ids_by_name(db_query_runner)

    # common neighbours c and d each have degree 2 -> 1/ln(2) + 1/ln(2)
    assert gds.topological_link_prediction.adamic_adar(ids["a"], ids["b"]) == pytest.approx(2 / math.log(2))


@pytest.mark.db_integration
def test_accepts_node_objects(gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner) -> None:
    nodes = _nodes_by_name(db_query_runner)

    assert gds.topological_link_prediction.common_neighbors(nodes["a"], nodes["b"]) == 2.0


@pytest.mark.db_integration
def test_relationship_query_and_direction(
    gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner
) -> None:
    ids = _node_ids_by_name(db_query_runner)

    # a and b only have outgoing FRIEND relationships, so there are no shared
    # neighbours when only considering incoming relationships.
    assert (
        gds.topological_link_prediction.common_neighbors(
            ids["a"], ids["b"], relationship_query="FRIEND", direction=Direction.INCOMING
        )
        == 0.0
    )
    assert (
        gds.topological_link_prediction.common_neighbors(
            ids["a"], ids["b"], relationship_query="FRIEND", direction=Direction.BOTH
        )
        == 2.0
    )


@pytest.mark.db_integration
def test_same_community(gds: AuraGraphDataScience, sample_graph: None, db_query_runner: Neo4jQueryRunner) -> None:
    ids = _node_ids_by_name(db_query_runner)

    assert gds.topological_link_prediction.same_community(ids["a"], ids["b"]) == 1.0
    assert gds.topological_link_prediction.same_community(ids["a"], ids["c"]) == 0.0
