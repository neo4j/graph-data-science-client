import uuid
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.projection_job_handle import ProjectionJobHandle
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


@pytest.fixture
def populated_db(query_runner: QueryRunner) -> Generator[None, None, None]:
    query_runner.run_cypher(
        "UNWIND range(1, 5) AS x CREATE (:Person)-[:KNOWS]->(:Person)",
        QueryType.USER_ACTION,
    )
    yield
    query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


@pytest.fixture
def endpoints(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> CatalogArrowEndpoints:
    return CatalogArrowEndpoints(arrow_client, query_runner)


@pytest.fixture
def cypher_handle(endpoints: CatalogArrowEndpoints) -> Generator[ProjectionJobHandle, None, None]:
    graph_name = f"projection-handle-{uuid.uuid4()}"
    handle = endpoints.project_async(
        graph_name=graph_name,
        query="UNWIND range(1, 10) AS x WITH gds.graph.project.remote(x, null) as g RETURN g",
    )
    try:
        yield handle
    finally:
        endpoints.drop(graph_name, fail_if_missing=False)


@pytest.fixture
def native_handle(endpoints: CatalogArrowEndpoints, populated_db: None) -> Generator[ProjectionJobHandle, None, None]:
    graph_name = f"projection-handle-native-{uuid.uuid4()}"
    handle = endpoints.project_native_async(
        graph_name=graph_name,
        node_label_filter=["Person"],
        relationship_type_filter=["KNOWS"],
    )
    try:
        yield handle
    finally:
        endpoints.drop(graph_name, fail_if_missing=False)


@pytest.mark.db_integration
class TestCypherProjectionJobHandle:
    def test_job_id_matches_started_job(self, cypher_handle: ProjectionJobHandle) -> None:
        assert cypher_handle.job_id()

    def test_wait_makes_status_succeed(self, cypher_handle: ProjectionJobHandle) -> None:
        cypher_handle.wait(log_progress=False)

        assert cypher_handle.status().succeeded()

    def test_done_returns_true_after_wait(self, cypher_handle: ProjectionJobHandle) -> None:
        cypher_handle.wait(log_progress=False)

        assert cypher_handle.done() is True

    def test_result_returns_graph_and_summary(self, cypher_handle: ProjectionJobHandle) -> None:
        graph, summary = cypher_handle.result()

        assert isinstance(graph, Graph)
        assert summary["nodeCount"] == 10
        assert summary["relationshipCount"] == 0

    def test_result_with_wait_false_after_wait_returns_summary(self, cypher_handle: ProjectionJobHandle) -> None:
        cypher_handle.wait(log_progress=False)

        graph, summary = cypher_handle.result(wait=False)

        assert isinstance(graph, Graph)
        assert summary["nodeCount"] == 10


@pytest.mark.db_integration
class TestNativeProjectionJobHandle:
    def test_job_id_matches_started_job(self, native_handle: ProjectionJobHandle) -> None:
        assert native_handle.job_id()

    def test_wait_makes_status_succeed(self, native_handle: ProjectionJobHandle) -> None:
        native_handle.wait(log_progress=False)

        assert native_handle.status().succeeded()

    def test_result_returns_graph_and_summary(self, native_handle: ProjectionJobHandle) -> None:
        graph, summary = native_handle.result()

        assert isinstance(graph, Graph)
        assert summary["nodeCount"] == 10
        assert summary["relationshipCount"] == 5
