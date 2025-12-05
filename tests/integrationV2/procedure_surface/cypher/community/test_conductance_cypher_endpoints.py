from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.conductance_cypher_endpoints import (
    ConductanceCypherEndpoints,
)
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
def conductance_endpoints(query_runner: QueryRunner) -> ConductanceCypherEndpoints:
    return ConductanceCypherEndpoints(query_runner)


def test_conductance_stream(conductance_endpoints: ConductanceCypherEndpoints, sample_graph: GraphV2) -> None:
    result = conductance_endpoints.stream(sample_graph, "community")

    assert set(result.columns) == {"community", "conductance"}
    assert len(result) == 2
