from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.config import FastRPConfig, GBClassifierConfig
from graphdatascience.procedure_surface.api.node_embedding.predict_endpoints import PredictWriteResult
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.predict_arrow_endpoints import PredictArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.train_arrow_endpoints import TrainArrowEndpoints
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)
from tests.integration.procedure_surface.arrow.node_embedding.conftest import ignore_preview_warning

graph = """
        CREATE
            (a1: A {x: 3.0, y: 0}),
            (a2: A {x: 3.0, y: 0}),
            (b1: B {x: 3.0}),
            (b2: B {x: 3.0}),
            (b3: B {x: 3.0}),
            (a1)-[:R]->(b1),
            (a1)-[:R]->(b2),
            (b2)-[:R]->(b3),
            (b3)-[:R]->(a1),
            (b3)-[:R]->(a2)
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
        "db_g",
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
def predict_endpoints(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[PredictArrowEndpoints, None, None]:
    yield PredictArrowEndpoints(arrow_client_runtime)


@pytest.fixture
def model_name(arrow_client_runtime: AuthenticatedArrowClient, sample_graph: Graph) -> Generator[str, None, None]:
    model_save_name = "my_model_123"
    _ = TrainArrowEndpoints(arrow_client_runtime)(
        G=sample_graph,
        feature_properties=["x"],
        graph_encoder=FastRPConfig(),
        decoder=GBClassifierConfig(),
        target_label="A",
        target_property="y",
        model_save_name=model_save_name,
    )
    yield model_save_name
    ModelCatalogArrowEndpoints(arrow_client_runtime).delete(model_save_name)


@pytest.fixture
def db_model_name(arrow_client_runtime: AuthenticatedArrowClient, db_graph: Graph) -> Generator[str, None, None]:
    model_save_name = "my_model_456"
    _ = TrainArrowEndpoints(arrow_client_runtime)(
        G=db_graph,
        feature_properties=["x"],
        graph_encoder=FastRPConfig(),
        decoder=GBClassifierConfig(),
        target_label="A",
        target_property="y",
        model_save_name=model_save_name,
    )
    yield model_save_name
    ModelCatalogArrowEndpoints(arrow_client_runtime).delete(model_save_name)


@ignore_preview_warning
def test_embeddings_predict_stream(
    predict_endpoints: PredictArrowEndpoints, sample_graph: Graph, model_name: str
) -> None:
    """Test FastPath stream operation."""
    result_df = predict_endpoints.stream(
        G=sample_graph,
        feature_properties=["x"],
        model_name=model_name,
    )

    assert "nodeId" in result_df.columns
    assert "prediction" in result_df.columns


@ignore_preview_warning
def test_embeddings_predict_mutate(
    predict_endpoints: PredictArrowEndpoints, sample_graph: Graph, model_name: str
) -> None:
    """Test FastPath mutate operation."""
    result = predict_endpoints.mutate(
        G=sample_graph,
        feature_properties=["x"],
        model_name=model_name,
        mutate_property="prediction123",
    )

    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is not None


@pytest.mark.db_integration
@ignore_preview_warning
def test_embeddings_predict_write(
    arrow_client_runtime: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph, db_model_name: str
) -> None:
    endpoints = PredictArrowEndpoints(arrow_client_runtime, WriteProtocol.select(arrow_client_runtime, query_runner))
    result = endpoints.write(
        G=db_graph,
        feature_properties=["x"],
        model_name=db_model_name,
        write_property="prediction123",
    )

    assert isinstance(result, PredictWriteResult)
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is not None

    assert (
        query_runner.run_cypher(
            "MATCH (n) WHERE n.prediction123 IS NOT NULL RETURN COUNT(*) AS count",
            query_type=QueryType.USER_ACTION,
        ).iloc[0, 0]
        == 5
    )
