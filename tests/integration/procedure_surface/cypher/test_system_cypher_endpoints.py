from typing import Generator

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.community.wcc_cypher_endpoints import WccCypherEndpoints
from graphdatascience.procedure_surface.cypher.list_progress_cypher_endpoint import ListProgressCypherEndpoint
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integration.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def list_progress_endpoint(query_runner: QueryRunner) -> Generator[ListProgressCypherEndpoint, None, None]:
    yield ListProgressCypherEndpoint(query_runner)


graph = """
        CREATE
            (a1)-[:T]->(a2)
        """

projection_query = """
    MATCH (n)-[r]->(m)
    WITH gds.graph.project('g', n, m, {}) AS G
    RETURN G
"""


@pytest.fixture(scope="class")
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph(query_runner, "g", graph, projection_query) as G:
        yield G


@pytest.fixture(scope="class")
def job_id(query_runner: QueryRunner, sample_graph: Graph) -> Generator[str, None, None]:
    job_id = "test_job_id"
    WccCypherEndpoints(query_runner).mutate(sample_graph, job_id=job_id, log_progress=True, mutate_property="wcc")
    yield job_id


@pytest.mark.skip(reason="Enable when we figure out how to retain jobs")
def test_list_progress_job_id(list_progress_endpoint: ListProgressCypherEndpoint, job_id: str) -> None:
    results = list_progress_endpoint(show_completed=True)

    assert len(results) == 1

    result = results[0]
    assert result.username == "neo4j"
    assert result.job_id == job_id
    assert "WCC" in result.task_name
    assert result.progress == "100%"
    assert "#" in result.progress_bar


def test_list_nothing(list_progress_endpoint: ListProgressCypherEndpoint) -> None:
    results = list_progress_endpoint()
    assert len(results) == 0
