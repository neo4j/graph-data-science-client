from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.modularity_cypher_endpoints import ModularityCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
              (a:Node {community: 0})
            , (b:Node {community: 0})
            , (c:Node {community: 0})
            , (d:Node {community: 1})
            , (e:Node {community: 1})
            , (f:Node {community: 1})
            ,  (a)-[:TYPE {weight: 1.0}]->(b)
            ,  (b)-[:TYPE {weight: 1.0}]->(c)
            ,  (c)-[:TYPE {weight: 1.0}]->(a)
            ,  (d)-[:TYPE {weight: 1.0}]->(e)
            ,  (e)-[:TYPE {weight: 1.0}]->(f)
            ,  (f)-[:TYPE {weight: 1.0}]->(d)
            ,  (a)-[:TYPE {weight: 2.0}]->(d)
            ,  (c)-[:TYPE {weight: 2.0}]->(e)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def modularity_endpoints(query_runner: QueryRunner) -> ModularityCypherEndpoints:
    return ModularityCypherEndpoints(query_runner)


def test_modularity_stats(modularity_endpoints: ModularityCypherEndpoints, sample_graph: GraphV2) -> None:
    result = modularity_endpoints.stats(sample_graph, "community")

    assert result.community_count == 2
    assert result.modularity == 0.21875
    assert result.node_count == 6
    assert result.relationship_count == 8


def test_modularity_stream(modularity_endpoints: ModularityCypherEndpoints, sample_graph: GraphV2) -> None:
    result = modularity_endpoints.stream(sample_graph, "community")

    assert set(result.columns) == {"communityId", "modularity"}
    assert len(result) == 2


def test_modularity_estimate(modularity_endpoints: ModularityCypherEndpoints, sample_graph: GraphV2) -> None:
    result = modularity_endpoints.estimate(sample_graph, "community")

    assert result.bytes_min > 0
    assert result.bytes_max >= result.bytes_min
