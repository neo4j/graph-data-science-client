from typing import Generator

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.memory_endpoints import MemoryListResult, MemorySummaryResult
from graphdatascience.procedure_surface.cypher.memory_cypher_endpoints import MemoryCypherEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integration.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_query = """
        CREATE
        (a:Node {name: 'a'}),
        (b:Node {name: 'b'}),
        (a)-[:REL]->(b)
    """

    projection_query = """
        MATCH (n:Node)-[r:REL]->(m:Node)
        WITH gds.graph.project('g', n, m) AS G
        RETURN G
    """

    with create_graph(query_runner, "g", create_query, projection_query) as g:
        yield g


@pytest.fixture
def memory_endpoints(query_runner: QueryRunner) -> MemoryCypherEndpoints:
    return MemoryCypherEndpoints(query_runner)


def test_summary(memory_endpoints: MemoryCypherEndpoints, sample_graph: Graph) -> None:
    result = memory_endpoints.summary()

    assert isinstance(result, list)
    assert len(result) >= 1
    entry = result[0]
    assert isinstance(entry, MemorySummaryResult)
    assert isinstance(entry.user, str)
    assert isinstance(entry.total_graphs_memory, int)
    assert isinstance(entry.total_tasks_memory, int)
    assert entry.total_graphs_memory > 0


def test_list(memory_endpoints: MemoryCypherEndpoints, sample_graph: Graph) -> None:
    result = memory_endpoints.list()

    assert isinstance(result, list)
    assert len(result) >= 1
    entry = next(e for e in result if e.name == "g")
    assert isinstance(entry, MemoryListResult)
    assert isinstance(entry.user, str)
    assert entry.entity is not None
    assert entry.memory_in_bytes > 0
