import uuid
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.protocol.write_protocols import (
    JobStatus,
    RemoteWriteBackV3,
    RemoteWriteBackV4,
    WriteProtocol,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph_from_db


@pytest.fixture
def projected_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    graph_data = """
        CREATE
        (a {prop: 42}),
        (b {prop: 43}),
        (c {prop: 44})
    """
    project_query = """
            MATCH (source)
            WITH gds.graph.project.remote(source, null, {sourceNodeProperties: properties(source), targetNodeProperties: null}) as g
            RETURN g
        """

    with create_graph_from_db(
        arrow_client,
        query_runner,
        f"write-protocol-{uuid.uuid4()}",
        graph_data,
        project_query,
    ) as G:
        yield G


def _start_property_export(arrow_client: AuthenticatedArrowClient, graph_name: str) -> str:
    return JobClient.run_job(
        arrow_client, "v2/graph.nodeProperties.stream", {"graphName": graph_name, "nodeProperties": ["prop"]}
    )


def _poll_until_done(protocol: WriteProtocol, job_id: str) -> JobStatus:
    """Drive the protocol manually until terminal — exercises start_job + get_status without WriteJobHandle."""
    import time

    deadline = time.time() + 30
    while time.time() < deadline:
        status = protocol.get_status(job_id)
        if status.done:
            return status
        time.sleep(0.1)
    raise AssertionError(f"Job '{job_id}' did not finish within timeout")


@pytest.mark.db_integration
def test_v3_start_job_then_poll_to_completion(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: Graph
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())
    protocol = RemoteWriteBackV3(arrow_client, query_runner)

    protocol.start_job(graph_name=projected_graph.name(), job_id=job_id, log_progress=False)
    status = _poll_until_done(protocol, job_id)

    assert status.done is True
    assert status.status == Status.COMPLETED.name
    assert status.written_node_properties == 3


@pytest.mark.db_integration
def test_v4_start_job_then_poll_to_completion(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: Graph
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())
    protocol = RemoteWriteBackV4(arrow_client, query_runner)

    protocol.start_job(graph_name=projected_graph.name(), job_id=job_id, log_progress=False)
    _poll_until_done(protocol, job_id)

    status = protocol.get_status(job_id)
    assert status.done is True
    assert status.status == Status.DONE.name
    assert status.written_node_properties == 3
