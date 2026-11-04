from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.embedding.config import (
    FastRPConfig,
    GraphSAGEConfig,
    MLPClassifierConfig,
)
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.embedding_endpoints import EmbeddingTrainResult
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.embedding_arrow_endpoints import EmbeddingArrowEndpoints
from graphdatascience.query_runner import QueryRunner
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
def embedding_endpoints(
    arrow_client_runtime: AuthenticatedArrowClient,
) -> Generator[EmbeddingArrowEndpoints, None, None]:
    yield EmbeddingArrowEndpoints(arrow_client_runtime)


MODEL_NAME = "my_model"
MLP_MODEL_NAME = "my_model_mlp"


@pytest.fixture(scope="module")
def training_graph(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    """Graph the shared model is trained on; distinct name so it can coexist with `sample_graph`."""
    with create_graph(arrow_client_runtime, "g-train", graph) as G:
        yield G


@pytest.fixture(scope="module")
def trained_model(
    arrow_client_runtime: AuthenticatedArrowClient, training_graph: Graph
) -> Generator[EmbeddingTrainResult, None, None]:
    # Trained once per module: training spawns a python-runtime container, and the create
    # tests only reference the model by name, so they can share it.
    endpoints = EmbeddingArrowEndpoints(arrow_client_runtime)
    train_result = endpoints.train(
        G=training_graph,
        input_properties=["x"],
        target_label="A",
        target_property="y",
        model_name=MODEL_NAME,
    )
    try:
        yield train_result
    finally:
        ModelCatalogArrowEndpoints(arrow_client_runtime).delete(model_name=MODEL_NAME, fail_if_missing=True)


@ignore_preview_warning
def test_embedding_create_default(arrow_client_runtime: AuthenticatedArrowClient, sample_graph: Graph) -> None:
    """Test create operation with FastRP"""
    embedding_endpoints = EmbeddingArrowEndpoints(arrow_client_runtime)
    result = embedding_endpoints.create(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is not None

    gds_arrow_client = GdsArrowClient(arrow_client_runtime)
    job_id = gds_arrow_client.get_node_properties(sample_graph.name(), node_properties=["embedding"])
    node_result = gds_arrow_client.stream_job(job_id)
    assert set(node_result.columns) == {"nodeId", "embedding"}


@ignore_preview_warning
def test_embedding_create_fastrp(arrow_client_runtime: AuthenticatedArrowClient, sample_graph: Graph) -> None:
    """Test create operation with defaults"""
    embedding_endpoints = EmbeddingArrowEndpoints(arrow_client_runtime)
    result = embedding_endpoints.create(
        G=sample_graph, graph_encoder=FastRPConfig(), embedding_dimension=63, mutate_property="embedding123"
    )

    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written > 0
    assert result.configuration is not None

    gds_arrow_client = GdsArrowClient(arrow_client_runtime)
    job_id = gds_arrow_client.get_node_properties(sample_graph.name(), node_properties=["embedding123"])
    node_result = gds_arrow_client.stream_job(job_id)
    assert set(node_result.columns) == {"nodeId", "embedding123"}
    assert len(node_result["embedding123"].iloc[0]) == 63


@ignore_preview_warning
def test_embedding_train_default(trained_model: EmbeddingTrainResult) -> None:
    """Test train operation with GraphSAGE defaults."""
    assert trained_model.compute_millis > 0


@ignore_preview_warning
def test_embedding_create_from_model(
    arrow_client_runtime: AuthenticatedArrowClient, trained_model: EmbeddingTrainResult, sample_graph: Graph
) -> None:
    """Test create operation with a previously trained GraphSAGE model."""
    embedding_endpoints = EmbeddingArrowEndpoints(arrow_client_runtime)
    create_result = embedding_endpoints.create(G=sample_graph, graph_encoder=MODEL_NAME, input_properties=["x"])

    assert create_result.compute_millis >= 0
    assert create_result.mutate_millis >= 0
    assert create_result.node_properties_written > 0
    assert create_result.configuration is not None


@ignore_preview_warning
def test_embedding_train_graphsage_mlp(arrow_client_runtime: AuthenticatedArrowClient, sample_graph: Graph) -> None:
    """Test train operation with a GraphSAGE encoder and MLP classifier decoder."""
    embedding_endpoints = EmbeddingArrowEndpoints(arrow_client_runtime)

    try:
        train_result = embedding_endpoints.train(
            G=sample_graph,
            input_properties=["x"],
            graph_encoder=GraphSAGEConfig(),
            decoder=MLPClassifierConfig(),
            target_label="A",
            target_property="y",
            model_name=MLP_MODEL_NAME,
        )
        assert train_result.compute_millis > 0
    finally:
        ModelCatalogArrowEndpoints(arrow_client_runtime).delete(model_name=MLP_MODEL_NAME, fail_if_missing=True)
