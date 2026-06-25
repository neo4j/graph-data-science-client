from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.fastpath_endpoints import FastPathWriteResult
from graphdatascience.procedure_surface.arrow.node_embedding.fastpath_arrow_endpoints import FastPathArrowEndpoints
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

# FastPath emits a preview warning on construction; tests that aren't asserting on it
# filter it out explicitly via this marker.
ignore_preview_warning = pytest.mark.filterwarnings("ignore:FastPath is a preview feature:UserWarning")

graph = """
        CREATE
            (a: Base),
            (b: Base),
            (e1: Event {time: 1, elapsed: 0}),
            (e2: Event {time: 2, elapsed: 1}),
            (e3: Event {time: 3, elapsed: 2}),
            (a)-[:HAS_EVENT]->(e1),
            (e1)-[:NEXT]->(e2),
            (b)-[:HAS_EVENT]->(e3)
        """


@pytest.fixture
def sample_graph(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client_runtime, "g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client_runtime: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph_from_db(
        arrow_client_runtime,
        query_runner,
        "g",
        graph,
        """
        MATCH (n)-[r]->(m)
        WITH gds.graph.project.remote(n, m, {
            sourceNodeLabels: labels(n), 
            sourceNodeProperties: properties(n), 
            targetNodeLabels: labels(m), 
            targetNodeProperties: properties(m), 
            relationshipType: type(r)
        }) as g
        RETURN g
        """,
    ) as g:
        yield g


@pytest.fixture
def fastpath_endpoints(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[FastPathArrowEndpoints, None, None]:
    yield FastPathArrowEndpoints(arrow_client_runtime)


@ignore_preview_warning
def test_fastpath_stream(fastpath_endpoints: FastPathArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastPath stream operation."""
    result_df = fastpath_endpoints.stream(
        G=sample_graph,
        base_node_label="Base",
        event_node_label="Event",
        dimension=16,
        max_elapsed_time=10,
        num_elapsed_times=4,
        first_relationship_type="HAS_EVENT",
        next_relationship_type="NEXT",
        time_node_property="time",
        output_time=3,
    )

    assert "nodeId" in result_df.columns
    assert "embeddings" in result_df.columns


@ignore_preview_warning
def test_fastpath_mutate(fastpath_endpoints: FastPathArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastPath mutate operation."""
    result = fastpath_endpoints.mutate(
        G=sample_graph,
        mutate_property="fastpath_embedding",
        base_node_label="Base",
        event_node_label="Event",
        dimension=16,
        max_elapsed_time=10,
        num_elapsed_times=4,
        first_relationship_type="HAS_EVENT",
        next_relationship_type="NEXT",
        time_node_property="time",
        output_time=3,
    )

    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is None  # wait for session release to change this


@pytest.mark.db_integration
@ignore_preview_warning
def test_fastpath_write(
    arrow_client_runtime: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph
) -> None:
    endpoints = FastPathArrowEndpoints(arrow_client_runtime, WriteProtocol.select(arrow_client_runtime, query_runner))
    result = endpoints.write(
        G=db_graph,
        write_property="fastpath_embedding",
        base_node_label="Base",
        event_node_label="Event",
        dimension=16,
        max_elapsed_time=10,
        num_elapsed_times=4,
        first_relationship_type="HAS_EVENT",
        next_relationship_type="NEXT",
        time_node_property="time",
        output_time=3,
    )

    assert isinstance(result, FastPathWriteResult)
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is None  # wait for session release to change this

    assert (
        query_runner.run_cypher(
            "MATCH (n) WHERE n.fastpath_embedding IS NOT NULL RETURN COUNT(*) AS count",
            query_type=QueryType.USER_ACTION,
        ).iloc[0, 0]
        == 2  # one embedding per Base node
    )


def test_fastpath_emits_preview_warning(arrow_client_runtime: AuthenticatedArrowClient) -> None:
    with pytest.warns(UserWarning, match="preview feature"):
        FastPathArrowEndpoints(arrow_client_runtime)
