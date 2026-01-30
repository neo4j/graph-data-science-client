from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.community.wcc_arrow_endpoints import WccArrowEndpoints
from graphdatascience.procedure_surface.arrow.system_arrow_endpoints import SystemArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

graph = """
        CREATE
            (a1)-[:T]->(a2)
        """


@pytest.fixture(scope="class")
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture(scope="class")
def job_id(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> Generator[str, None, None]:
    job_id = "test_job_id"
    WccArrowEndpoints(arrow_client).stats(sample_graph, job_id=job_id)
    yield job_id


@pytest.fixture
def system_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[SystemArrowEndpoints, None, None]:
    yield SystemArrowEndpoints(arrow_client)


def test_list_progress_job_id(system_endpoints: SystemArrowEndpoints, job_id: str) -> None:
    results = system_endpoints.list_progress(job_id=job_id, show_completed=True)

    assert len(results) == 1

    result = results[0]
    assert result.username == "neo4j"
    assert result.job_id == job_id
    assert "WCC" in result.task_name
    assert result.progress == "100%"
    assert "#" in result.progress_bar


def test_list_nothing(system_endpoints: SystemArrowEndpoints) -> None:
    results = system_endpoints.list_progress()
    assert len(results) == 0
