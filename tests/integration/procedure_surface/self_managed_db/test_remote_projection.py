"""Remote projections from a stock Neo4j database (no GDS plugin) into a GDS session.

Neo4j core ships remote-projection support (`gds.arrow.project.v3`,
`gds.graph.project.remote`), so a plugin-free Neo4j can project into a GDS session.
These tests verify that path, in contrast to
`procedure_surface/arrow/test_project_protocols.py` which runs against an image with
the stubs force-disabled. Note that the stock stubs only implement protocol version
v3 (the dbms reports `v1, v2, v3` via `gds.session.dbms.protocol.version`); the v4
procedures (`gds.arrow.project.cypher.v4`, `gds.arrow.project.store.v4`) require the
GDS plugin.
"""

import uuid
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.session.remote_ops.project_protocols import ProjectProtocolV3
from graphdatascience.session.remote_ops.projection_runner import ProjectionRunner


@pytest.fixture
def populated_db(query_runner: QueryRunner, neo4j_connection) -> Generator[None, None, None]:
    query_runner.run_cypher(
        "UNWIND range(1, 5) AS x CREATE (:Person)-[:KNOWS]->(:Person)",
        QueryType.USER_ACTION,
    )
    yield
    qr =    Neo4jQueryRunner.create_for_db(
        f"bolt://{neo4j_connection.uri}",
        ("neo4j", "password"),
    )
    qr.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)
    qr.close()

@pytest.mark.db_integration
def test_v3_run_cypher_projection(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, populated_db: None
) -> None:
    graph_name = f"std-db-v3-cypher-{uuid.uuid4()}"
    catalog = CatalogArrowEndpoints(arrow_client)
    protocol = ProjectProtocolV3(arrow_client, query_runner, TerminationFlagNoop())

    try:
        result = ProjectionRunner(protocol, arrow_client, TerminationFlagNoop()).run_cypher_projection(
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
def test_project_endpoints(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    populated_db: None,
) -> None:
    endpoints = CatalogArrowEndpoints(arrow_client, query_runner)
    graph_name = f"std-db-endpoints-{uuid.uuid4()}"

    try:
        G, result = endpoints.project.cypher(
            graph_name=graph_name,
            query="MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)",
        )

        assert G.name() == graph_name
        assert result.node_count == 10
        assert result.relationship_count == 5
        assert len(endpoints.list(graph_name)) == 1
    finally:
        endpoints.drop(graph_name, fail_if_missing=False)
