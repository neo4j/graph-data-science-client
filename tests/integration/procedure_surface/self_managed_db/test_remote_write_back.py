"""Write-back from a GDS session into a stock Neo4j database (no GDS plugin).

Neo4j core ships the remote write-back stub (`gds.arrow.write.v3`), so a plugin-free
Neo4j can receive results from a GDS session. These tests verify that path, in contrast to
`procedure_surface/arrow/test_write_protocols.py` which runs against an image with the
stubs force-disabled. As with projection, only protocol version v3 is available without the
GDS plugin, so `WriteProtocol.select` must resolve to `RemoteWriteBackV3`.

Known gaps of the stock stubs (Neo4j 2026.07.1), each pinned by a strict xfail below:

* `gds.arrow.write.v3` rejects the `writeProperties` and `writeRelationshipType`
  configuration keys that the client sends for property / relationship-type overrides.
  Only write-backs that keep the session-side names work.
* Graphs projected via the stock stub land in the session through the legacy
  `v1/CREATE_GRAPH_FROM_TRIPLETS` path. Algorithm jobs find such graphs, but the catalog
  export jobs (`v2/graph.nodeProperties.stream`, `v2/graph.relationships.stream`) fail
  with "Graph ... does not exist on database `neo4j`".
"""

import time
import uuid
from typing import Any, Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import get_graph
from graphdatascience.procedure_surface.arrow.catalog.node_properties_arrow_endpoints import (
    NodePropertiesArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.catalog.relationship_arrow_endpoints import (
    RelationshipArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.wcc_arrow_endpoints import WccArrowEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.session.remote_ops.project_protocols import ProjectProtocolV3
from graphdatascience.session.remote_ops.projection_runner import ProjectionRunner
from graphdatascience.session.remote_ops.status import Status
from graphdatascience.session.remote_ops.write_protocols import (
    JobStatus,
    RemoteWriteBackV3,
    WriteProtocol,
)

GRAPH_DATA = """
    CREATE
    (a:Node {prop: 42}),
    (b:Node {prop: 43}),
    (c:Node {prop: 44}),
    (a)-[:REL {weight: 1.0}]->(b),
    (b)-[:REL {weight: 2.0}]->(c)
"""

PROJECTION_QUERY = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    WITH gds.graph.project.remote(
        n,
        m,
        {
            sourceNodeProperties: properties(n),
            targetNodeProperties: properties(m),
            relationshipType: type(r),
            relationshipProperties: {weight: r.weight}
        }
    ) AS g
    RETURN g
"""


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    """Project via the v3 protocol directly; the stock stubs only speak v3."""
    graph_name = f"std-db-write-{uuid.uuid4()}"
    catalog = CatalogArrowEndpoints(arrow_client)
    try:
        query_runner.run_cypher(GRAPH_DATA, QueryType.USER_ACTION)
        protocol = ProjectProtocolV3(arrow_client, query_runner, TerminationFlagNoop())
        ProjectionRunner(protocol, arrow_client, TerminationFlagNoop()).run_cypher_projection(
            graph_name=graph_name, query=PROJECTION_QUERY, job_id=str(uuid.uuid4())
        )
        yield get_graph(graph_name, arrow_client)
    finally:
        catalog.drop(graph_name, fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


@pytest.fixture
def write_protocol(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> WriteProtocol:
    return WriteProtocol.select(arrow_client, query_runner)


def _count(query_runner: QueryRunner, query: str) -> Any:
    return query_runner.run_cypher(query, query_type=QueryType.USER_ACTION).iloc[0, 0]


def _poll_until_done(protocol: WriteProtocol, job_id: str) -> JobStatus:
    deadline = time.time() + 30
    while time.time() < deadline:
        status = protocol.get_status(job_id)
        if status.done:
            return status
        time.sleep(0.1)
    raise AssertionError(f"Job '{job_id}' did not finish within timeout")


@pytest.mark.db_integration
def test_select_resolves_to_v3(write_protocol: WriteProtocol) -> None:
    assert isinstance(write_protocol, RemoteWriteBackV3)


@pytest.mark.db_integration
def test_v3_start_job_then_poll_to_completion(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph
) -> None:
    """Drive the protocol manually: WCC on the session, then write `componentId` back unchanged."""
    job_id = JobClient.run_job_and_wait(
        arrow_client, "v2/community.wcc", {"graphName": db_graph.name()}, show_progress=False
    )
    protocol = RemoteWriteBackV3(arrow_client, query_runner)

    protocol.start_job(graph_name=db_graph.name(), job_id=job_id, log_progress=False)
    status = _poll_until_done(protocol, job_id)

    assert status.done is True
    assert status.status == Status.COMPLETED.name
    assert status.written_node_properties == 3
    assert _count(query_runner, "MATCH (n:Node) WHERE n.componentId IS NOT NULL RETURN count(n)") == 3


@pytest.mark.db_integration
def test_algorithm_write(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    write_protocol: WriteProtocol,
    db_graph: Graph,
) -> None:
    endpoints = WccArrowEndpoints(arrow_client, write_protocol)

    result = endpoints.write(G=db_graph, write_property="wccId")

    assert result.component_count == 1
    assert result.node_properties_written == 3
    assert _count(query_runner, "MATCH (n:Node) WHERE n.componentId IS NOT NULL RETURN count(n)") == 3


@pytest.mark.db_integration
def test_write_node_properties(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph
) -> None:
    endpoints = NodePropertiesArrowEndpoints(arrow_client, query_runner)

    result = endpoints.write(G=db_graph, node_properties=["prop"])

    assert result.graph_name == db_graph.name()
    assert result.properties_written == 3


@pytest.mark.db_integration
def test_write_relationships(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    write_protocol: WriteProtocol,
    db_graph: Graph,
) -> None:
    endpoints = RelationshipArrowEndpoints(arrow_client, write_protocol)

    result = endpoints.write(G=db_graph, relationship_type="REL", relationship_properties=["weight"])

    assert result.graph_name == db_graph.name()
    assert result.relationships_written == 2
    assert result.properties_written == 2
