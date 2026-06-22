import math
from typing import Generator

import pytest
from neo4j.graph import Node

from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.procedure_surface.api.topological_link_prediction_endpoints import Direction
from graphdatascience.procedure_surface.cypher.topological_link_prediction_cypher_endpoints import (
    TopologicalLinkPredictionCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[None, None, None]:
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
        query_runner.run_cypher(create_query, QueryType.USER_ACTION)
        yield
    finally:
        query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


@pytest.fixture
def endpoints(query_runner: QueryRunner) -> TopologicalLinkPredictionCypherEndpoints:
    return TopologicalLinkPredictionCypherEndpoints(query_runner)


@filter_id_func_deprecation_warning()
def _node_ids_by_name(query_runner: QueryRunner) -> dict[str, int]:
    df = query_runner.run_cypher("MATCH (n:Person) RETURN id(n) AS id, n.name AS name", QueryType.USER_ACTION)
    return {row["name"]: int(row["id"]) for _, row in df.iterrows()}


def _nodes_by_name(query_runner: QueryRunner) -> dict[str, Node]:
    df = query_runner.run_cypher("MATCH (n:Person) RETURN n AS node, n.name AS name", QueryType.USER_ACTION)
    return {row["name"]: row["node"] for _, row in df.iterrows()}


def test_common_neighbors(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    ids = _node_ids_by_name(query_runner)

    assert endpoints.common_neighbors(ids["a"], ids["b"]) == 2.0


def test_total_neighbors(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    ids = _node_ids_by_name(query_runner)

    assert endpoints.total_neighbors(ids["a"], ids["b"]) == 2.0


def test_preferential_attachment(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    ids = _node_ids_by_name(query_runner)

    assert endpoints.preferential_attachment(ids["a"], ids["b"]) == 4.0


def test_resource_allocation(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    ids = _node_ids_by_name(query_runner)

    # common neighbours c and d each have degree 2 -> 1/2 + 1/2
    assert endpoints.resource_allocation(ids["a"], ids["b"]) == pytest.approx(1.0)


def test_adamic_adar(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    ids = _node_ids_by_name(query_runner)

    # common neighbours c and d each have degree 2 -> 1/ln(2) + 1/ln(2)
    assert endpoints.adamic_adar(ids["a"], ids["b"]) == pytest.approx(2 / math.log(2))


def test_accepts_node_objects(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    nodes = _nodes_by_name(query_runner)

    assert endpoints.common_neighbors(nodes["a"], nodes["b"]) == 2.0


def test_relationship_query_and_direction(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    ids = _node_ids_by_name(query_runner)

    # a and b only have outgoing FRIEND relationships, so there are no shared
    # neighbours when only considering incoming relationships.
    assert (
        endpoints.common_neighbors(ids["a"], ids["b"], relationship_query="FRIEND", direction=Direction.INCOMING) == 0.0
    )
    assert endpoints.common_neighbors(ids["a"], ids["b"], relationship_query="FRIEND", direction=Direction.BOTH) == 2.0


def test_same_community(
    endpoints: TopologicalLinkPredictionCypherEndpoints, sample_graph: None, query_runner: QueryRunner
) -> None:
    ids = _node_ids_by_name(query_runner)

    assert endpoints.same_community(ids["a"], ids["b"]) == 1.0
    assert endpoints.same_community(ids["a"], ids["c"]) == 0.0
