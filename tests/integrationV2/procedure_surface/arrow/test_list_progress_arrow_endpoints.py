from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.community.wcc_arrow_endpoints import WccArrowEndpoints
from graphdatascience.procedure_surface.arrow.list_progress_arrow_endpoint import ListProgressArrowEndpoint
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

graph = """
        CREATE
            (a1)-[:T]->(a2)
        """


@pytest.fixture(scope="class")
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture(scope="class")
def job_id(arrow_client: AuthenticatedArrowClient, sample_graph: Graph) -> Generator[str, None, None]:
    job_id = "test_job_id"
    WccArrowEndpoints(arrow_client).stats(sample_graph, job_id=job_id)
    yield job_id


@pytest.fixture
def list_progress_endpoint(arrow_client: AuthenticatedArrowClient) -> Generator[ListProgressArrowEndpoint, None, None]:
    yield ListProgressArrowEndpoint(arrow_client)


def test_list_progress_job_id(list_progress_endpoint: ListProgressArrowEndpoint, job_id: str) -> None:
    results = list_progress_endpoint(job_id=job_id, show_completed=True)

    assert len(results) == 1

    result = results[0]
    assert result.username == "neo4j"
    assert result.job_id == job_id
    assert "WCC" in result.task_name
    assert result.progress == "100%"
    assert "#" in result.progress_bar


def test_list_nothing(list_progress_endpoint: ListProgressArrowEndpoint) -> None:
    results = list_progress_endpoint()
    assert len(results) == 0
