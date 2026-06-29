import uuid
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.session.remote_ops.project_protocols import (
    ProjectProtocol,
    ProjectProtocolV3,
    ProjectProtocolV4,
)
from graphdatascience.session.remote_ops.projection_runner import ProjectionRunner


@pytest.fixture
def populated_db(query_runner: QueryRunner) -> Generator[None, None, None]:
    query_runner.run_cypher(
        "UNWIND range(1, 5) AS x CREATE (:Person)-[:KNOWS]->(:Person)",
        QueryType.USER_ACTION,
    )
    yield
    query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


def _runner(protocol: ProjectProtocol, arrow_client: AuthenticatedArrowClient) -> ProjectionRunner:
    return ProjectionRunner(protocol, arrow_client, TerminationFlagNoop())


@pytest.mark.db_integration
def test_v3_run_cypher_projection(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, populated_db: None
) -> None:
    graph_name = f"v3-cypher-{uuid.uuid4()}"
    catalog = CatalogArrowEndpoints(arrow_client)
    protocol = ProjectProtocolV3(arrow_client, query_runner, TerminationFlagNoop())

    try:
        result = _runner(protocol, arrow_client).run_cypher_projection(
            graph_name=graph_name,
            query="MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)",
            job_id=str(uuid.uuid4()),
        )

        assert isinstance(result, dict)
        listed = catalog.list(graph_name)
        assert len(listed) == 1
        assert listed[0].graph_name == graph_name
        assert listed[0].node_count == 10
        assert listed[0].relationship_count == 5
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.db_integration
def test_v4_run_cypher_projection(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, populated_db: None
) -> None:
    graph_name = f"v4-cypher-{uuid.uuid4()}"
    catalog = CatalogArrowEndpoints(arrow_client)
    protocol = ProjectProtocolV4(arrow_client, query_runner, TerminationFlagNoop())

    try:
        result = _runner(protocol, arrow_client).run_cypher_projection(
            graph_name=graph_name,
            query="MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)",
            job_id=str(uuid.uuid4()),
        )

        assert isinstance(result, dict)
        listed = catalog.list(graph_name)
        assert len(listed) == 1
        assert listed[0].graph_name == graph_name
        assert listed[0].node_count == 10
        assert listed[0].relationship_count == 5
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


@pytest.mark.db_integration
def test_v4_run_store_projection(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, populated_db: None
) -> None:
    graph_name = f"v4-store-{uuid.uuid4()}"
    catalog = CatalogArrowEndpoints(arrow_client)
    protocol = ProjectProtocolV4(arrow_client, query_runner, TerminationFlagNoop())

    try:
        result = _runner(protocol, arrow_client).run_store_projection(
            graph_name=graph_name,
            node_label_filter=["Person"],
            relationship_type_filter=["KNOWS"],
            job_id=str(uuid.uuid4()),
        )

        assert isinstance(result, dict)
        listed = catalog.list(graph_name)
        assert len(listed) == 1
        assert listed[0].graph_name == graph_name
        assert listed[0].node_count == 10
        assert listed[0].relationship_count == 5
    finally:
        catalog.drop(graph_name, fail_if_missing=False)
