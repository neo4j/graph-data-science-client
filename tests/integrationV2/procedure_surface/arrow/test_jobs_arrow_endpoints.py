import uuid
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.projection_job_handle import ProjectionJobHandle
from graphdatascience.procedure_surface.api.write_job_handle import WriteJobHandle
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.jobs_arrow_endpoints import (
    JobNotFoundException,
    JobsArrowEndpoints,
)
from graphdatascience.query_runner import QueryRunner
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol
from graphdatascience.query_runner.query_type import QueryType
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph, create_graph_from_db

GDL = """
    CREATE
        (a:Node {id: 0}),
        (b:Node {id: 1}),
        (c:Node {id: 2}),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
    """

DB_GDL = """
    CREATE
        (a {prop: 42}),
        (b {prop: 43}),
        (c {prop: 44}),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
    """

DB_PROJECT_QUERY = """
    MATCH (source)-[r]->(target)
    WITH gds.graph.project.remote(source, target, {sourceNodeProperties: properties(source), targetNodeProperties: properties(target)}) AS g
    RETURN g
"""


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, f"jobs-endpoints-{uuid.uuid4()}", GDL) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        f"jobs-endpoints-db-{uuid.uuid4()}",
        DB_GDL,
        DB_PROJECT_QUERY,
    ) as G:
        yield G


def _start_pagerank(arrow_client: AuthenticatedArrowClient, graph: GraphV2) -> str:
    return JobClient.run_job(arrow_client, "v2/centrality.pageRank", {"graphName": graph.name()})


def test_get_raises_when_job_id_unknown(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    endpoints = JobsArrowEndpoints(arrow_client)

    with pytest.raises(JobNotFoundException, match="not found"):
        endpoints.get(sample_graph, str(uuid.uuid4()))


def test_get_returns_basic_job_handle_when_no_write_protocol(
    arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2
) -> None:
    job_id = _start_pagerank(arrow_client, sample_graph)
    endpoints = JobsArrowEndpoints(arrow_client)

    handle = endpoints.get(sample_graph, job_id)

    assert isinstance(handle, JobHandle)
    assert not isinstance(handle, (ProjectionJobHandle, WriteJobHandle))
    assert handle.job_id() == job_id


@pytest.mark.db_integration
def test_get_returns_projection_handle_for_projection_job(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> None:
    graph_name = f"projection-job-{uuid.uuid4()}"
    catalog = CatalogArrowEndpoints(arrow_client, query_runner)

    try:
        query_runner.run_cypher(DB_GDL, QueryType.USER_ACTION)

        projection_handle = catalog.project_async(graph_name=graph_name, query=DB_PROJECT_QUERY)
        projection_handle.wait(log_progress=False)
        G, _ = projection_handle.result()

        endpoints = JobsArrowEndpoints(arrow_client)
        handle = endpoints.get(G, projection_handle.job_id())

        assert isinstance(handle, ProjectionJobHandle)
        assert handle.job_id() == projection_handle.job_id()
    finally:
        catalog.drop(graph_name, fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


@pytest.mark.db_integration
def test_get_returns_write_job_handle_when_write_protocol_succeeds(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    # Run a pagerank algorithm so we have a result to write back.
    algo_job_id = _start_pagerank(arrow_client, db_graph)
    JobClient().wait_for_job(arrow_client, algo_job_id, show_progress=False)

    write_protocol = WriteProtocol.select(arrow_client, query_runner)
    write_protocol.start_job(
        graph_name=db_graph.name(),
        job_id=algo_job_id,
        property_overwrites={"pagerank": "pagerank"},
        log_progress=False,
    )

    endpoints = JobsArrowEndpoints(arrow_client, write_protocol=write_protocol)

    handle = endpoints.get(db_graph, algo_job_id)

    assert isinstance(handle, WriteJobHandle)
    assert handle.job_id() == algo_job_id
