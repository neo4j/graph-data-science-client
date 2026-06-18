from typing import Generator

import pytest

from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.util_cypher_endpoints import UtilCypherEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from tests.integration.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_query = """
        CREATE
        (a:Node {name: 'a', rank: 1}),
        (b:Node {name: 'b', rank: 2}),
        (a)-[:REL]->(b)
    """

    projection_query = """
        MATCH (n:Node)-[r:REL]->(m:Node)
        WITH gds.graph.project('g', n, m, {
            sourceNodeProperties: n { .rank },
            targetNodeProperties: m { .rank }
        }) AS G
        RETURN G
    """

    with create_graph(query_runner, "g", create_query, projection_query) as g:
        yield g


@pytest.fixture
def util_endpoints(query_runner: Neo4jQueryRunner) -> UtilCypherEndpoints:
    return UtilCypherEndpoints(query_runner)


@filter_id_func_deprecation_warning()
def _node_ids_by_name(query_runner: QueryRunner) -> dict[str, int]:
    df = query_runner.run_cypher("MATCH (n:Node) RETURN id(n) AS id, n.name AS name", QueryType.USER_ACTION)
    return {row["name"]: int(row["id"]) for _, row in df.iterrows()}


def test_as_node(util_endpoints: UtilCypherEndpoints, sample_graph: Graph, query_runner: QueryRunner) -> None:
    ids = _node_ids_by_name(query_runner)

    node = util_endpoints.as_node(ids["a"])

    assert node["name"] == "a"
    assert node["rank"] == 1


def test_as_nodes(util_endpoints: UtilCypherEndpoints, sample_graph: Graph, query_runner: QueryRunner) -> None:
    ids = _node_ids_by_name(query_runner)

    nodes = util_endpoints.as_nodes([ids["a"], ids["b"]])

    assert sorted(node["name"] for node in nodes) == ["a", "b"]


def test_node_property(util_endpoints: UtilCypherEndpoints, sample_graph: Graph, query_runner: QueryRunner) -> None:
    ids = _node_ids_by_name(query_runner)

    assert util_endpoints.node_property(sample_graph, ids["a"], "rank") == 1
    assert util_endpoints.node_property(sample_graph, ids["b"], "rank") == 2


def test_one_hot_encoding(util_endpoints: UtilCypherEndpoints) -> None:
    assert util_endpoints.one_hot_encoding(["a", "b", "c"], ["b"]) == [0, 1, 0]
