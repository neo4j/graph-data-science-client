from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.config import FastRPConfig
from graphdatascience.procedure_surface.api.node_embedding.encode_endpoints import EncodeWriteResult
from graphdatascience.procedure_surface.arrow.node_embedding.encode_arrow_endpoints import EncodeArrowEndpoints
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)
from tests.integration.procedure_surface.arrow.node_embedding.conftest import ignore_preview_warning

graph = """
        CREATE
            (a1: A {x: 3.0}),
            (a2: A {x: 3.0}),
            (b1: B {x: 3.0}),
            (b2: B {x: 3.0}),
            (b3: B {x: 3.0}),
            (a1)-[:R]->(b1),
            (a1)-[:R]->(b2),
            (b2)-[:R]->(b3),
            (b3)-[:R]->(a1)
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
def encode_endpoints(
    arrow_client_runtime: AuthenticatedArrowClient,
) -> Generator[EncodeArrowEndpoints, None, None]:
    yield EncodeArrowEndpoints(arrow_client_runtime)


@ignore_preview_warning
def test_embeddings_encode_stream(encode_endpoints: EncodeArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastPath stream operation."""
    result_df = encode_endpoints.stream(
        G=sample_graph,
        graph_encoder=FastRPConfig(),
    )

    assert "nodeId" in result_df.columns
    assert "embeddings" in result_df.columns


@ignore_preview_warning
def test_embeddings_encode_mutate(encode_endpoints: EncodeArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastPath mutate operation."""
    result = encode_endpoints.mutate(
        G=sample_graph,
        graph_encoder=FastRPConfig(),
        mutate_property="embedding123",
    )

    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is not None


@pytest.mark.db_integration
@ignore_preview_warning
def test_embeddings_encode_write(
    arrow_client_runtime: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph
) -> None:
    endpoints = EncodeArrowEndpoints(arrow_client_runtime, WriteProtocol.select(arrow_client_runtime, query_runner))
    result = endpoints.write(
        G=db_graph,
        graph_encoder=FastRPConfig(),
        write_property="embedding123",
    )

    assert isinstance(result, EncodeWriteResult)
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is not None

    assert (
        query_runner.run_cypher(
            "MATCH (n) WHERE n.embedding123 IS NOT NULL RETURN COUNT(*) AS count",
            query_type=QueryType.USER_ACTION,
        ).iloc[0, 0]
        == 4
    )
