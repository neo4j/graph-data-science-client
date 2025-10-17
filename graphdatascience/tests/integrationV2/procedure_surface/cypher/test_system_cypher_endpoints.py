from typing import Generator

import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.wcc_cypher_endpoints import WccCypherEndpoints
from graphdatascience.procedure_surface.cypher.system_cypher_endpoints import SystemCypherEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def system_endpoints(query_runner: QueryRunner) -> Generator[SystemCypherEndpoints, None, None]:
    yield SystemCypherEndpoints(query_runner)


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
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph(query_runner, "g", graph, projection_query) as G:
        yield G


@pytest.fixture(scope="class")
def job_id(query_runner: QueryRunner, sample_graph: GraphV2) -> Generator[str, None, None]:
    job_id = "test_job_id"
    WccCypherEndpoints(query_runner).mutate(sample_graph, job_id=job_id, log_progress=True, mutate_property="wcc")
    yield job_id


@pytest.mark.skip(reason="Enable when we figure out how to retain jobs")
def test_list_progress_job_id(system_endpoints: SystemCypherEndpoints, job_id: str) -> None:
    results = system_endpoints.list_progress(show_completed=True)

    assert len(results) == 1

    result = results[0]
    assert result.username == "neo4j"
    assert result.job_id == job_id
    assert "WCC" in result.task_name
    assert result.progress == "100%"
    assert "#" in result.progress_bar


def test_list_nothing(system_endpoints: SystemCypherEndpoints) -> None:
    results = system_endpoints.list_progress()
    assert len(results) == 0
