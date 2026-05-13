from typing import Generator
from uuid import uuid4

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.pipeline.link_prediction_pipeline_arrow_endpoints import (
    LinkPredictionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_catalog_arrow_endpoints import (
    PipelineCatalogArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph, create_graph_from_db

graph = """
    CREATE
    (a: Node {feature: 1.0}),
    (b: Node {feature: 2.0}),
    (c: Node {feature: 3.0}),
    (d: Node {feature: 4.0}),
    (e: Node {feature: 5.0}),
    (f: Node {feature: 6.0}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(e),
    (e)-[:REL]->(f),
    (f)-[:REL]->(a),
    (a)-[:CONTEXTREL]->(c),
    (b)-[:CONTEXTREL]->(d),
    (c)-[:CONTEXTREL]->(e),
    (d)-[:CONTEXTREL]->(f)
"""


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(
        arrow_client,
        f"link-prediction-g-{uuid4().hex[:8]}",
        graph,
        undirected=("REL", "REL_UNDIRECTED"),
    ) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        f"link-prediction-db-g-{uuid4().hex[:8]}",
        graph,
        """
            MATCH (n)-[r]->(m)
            WITH gds.graph.project.remote(n, m, {
                sourceNodeLabels: labels(n),
                targetNodeLabels: labels(m),
                sourceNodeProperties: properties(n),
                targetNodeProperties: properties(m),
                relationshipType: type(r)
            }) AS g
            RETURN g
        """,
        undirected_relationship_types=["REL"],
    ) as g:
        yield g


@pytest.fixture
def endpoints(arrow_client: AuthenticatedArrowClient) -> LinkPredictionPipelineArrowEndpoints:
    return LinkPredictionPipelineArrowEndpoints(arrow_client, None, show_progress=False)


def test_link_prediction_train_and_predict_stream(
    arrow_client: AuthenticatedArrowClient,
    endpoints: LinkPredictionPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"lp-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-model-{uuid4().hex[:8]}"

    try:
        pipeline, create_result = endpoints.create(pipeline_name)
        node_property_result = pipeline.add_node_property("degree", mutate_property="rank")
        feature_result = pipeline.add_feature("l2", node_properties=["rank"])
        split_result = pipeline.configure_split(train_fraction=0.7, test_fraction=0.2, validation_folds=2)
        candidate_result = pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, train_result = pipeline.train(
            sample_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL_UNDIRECTED",
        )
        stream_result = model.predict_stream(
            sample_graph,
            source_node_label="Node",
            target_node_label="Node",
            top_n=2,
        )
        fetched_pipeline = endpoints.get(pipeline_name)

        assert create_result.name == pipeline_name
        assert node_property_result.name == pipeline_name
        assert feature_result.name == pipeline_name
        assert split_result.name == pipeline_name
        assert candidate_result.name == pipeline_name
        assert train_result.train_millis is not None
        assert train_result.train_millis >= 0
        assert model.exists()
        assert fetched_pipeline.name() == pipeline_name
        assert "node1" in stream_result.columns or "sourceNodeId" in stream_result.columns
        assert "node2" in stream_result.columns or "targetNodeId" in stream_result.columns
        assert "probability" in stream_result.columns
        assert len(stream_result) > 0
    finally:
        ModelCatalogArrowEndpoints(arrow_client).drop(model_name)
        PipelineCatalogArrowEndpoints(arrow_client).drop(pipeline_name)


def test_link_prediction_train_estimate(
    arrow_client: AuthenticatedArrowClient,
    endpoints: LinkPredictionPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"lp-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.add_feature("l2", node_properties=["feature"])
        pipeline.configure_split(train_fraction=0.5, test_fraction=0.3, validation_folds=2)
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        estimate = pipeline.train_estimate(
            sample_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL_UNDIRECTED",
        )

        assert estimate.required_memory is not None
        assert estimate.bytes_max is None or estimate.bytes_max >= 0
    finally:
        ModelCatalogArrowEndpoints(arrow_client).drop(model_name)
        PipelineCatalogArrowEndpoints(arrow_client).drop(pipeline_name)


def test_link_prediction_predict_estimate(
    arrow_client: AuthenticatedArrowClient,
    endpoints: LinkPredictionPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"lp-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.add_feature("l2", node_properties=["feature"])
        pipeline.configure_split(train_fraction=0.5, test_fraction=0.3, validation_folds=2)
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, _ = pipeline.train(
            sample_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL_UNDIRECTED",
        )
        estimate = model.predict_estimate(
            sample_graph,
            source_node_label="Node",
            target_node_label="Node",
            top_n=2,
        )

        assert estimate.required_memory is not None
        assert estimate.bytes_max is None or estimate.bytes_max >= 0
    finally:
        ModelCatalogArrowEndpoints(arrow_client).drop(model_name)
        PipelineCatalogArrowEndpoints(arrow_client).drop(pipeline_name)


@pytest.mark.db_integration
def test_link_prediction_predict_mutate(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    db_graph: GraphV2,
) -> None:
    endpoints = LinkPredictionPipelineArrowEndpoints(
        arrow_client,
        RemoteWriteBackClient.create(arrow_client, query_runner),
        show_progress=False,
    )

    pipeline_name = f"lp-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.add_feature("l2", node_properties=["feature"])
        pipeline.configure_split(train_fraction=0.5, test_fraction=0.3, validation_folds=2)
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, _ = pipeline.train(
            db_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL",
        )

        mutate_result = model.predict_mutate(
            db_graph,
            mutate_relationship_type="PREDICTED_REL",
            source_node_label="Node",
            target_node_label="Node",
            top_n=2,
        )

        assert mutate_result.relationships_written is not None
        assert mutate_result.relationships_written > 0
        assert mutate_result.mutate_millis is not None
        assert mutate_result.mutate_millis >= 0
        assert mutate_result.configuration and mutate_result.configuration["mutateRelationshipType"] == "PREDICTED_REL"
    finally:
        ModelCatalogArrowEndpoints(arrow_client).drop(model_name)
        PipelineCatalogArrowEndpoints(arrow_client).drop(pipeline_name)


def test_link_prediction_pipeline_object_supports_exists_and_drop(
    arrow_client: AuthenticatedArrowClient,
    endpoints: LinkPredictionPipelineArrowEndpoints,
) -> None:
    pipeline_name = f"lp-pipe-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)

        assert pipeline.exists() is True
        dropped = pipeline.drop()
        assert dropped is not None
        assert dropped.pipeline_name == pipeline_name
        assert pipeline.exists() is False
    finally:
        PipelineCatalogArrowEndpoints(arrow_client).drop(pipeline_name)


def test_link_prediction_configure_auto_tuning(
    arrow_client: AuthenticatedArrowClient,
    endpoints: LinkPredictionPipelineArrowEndpoints,
) -> None:
    pipeline_name = f"lp-pipe-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        result = pipeline.configure_auto_tuning(max_trials=42)

        assert result.name == pipeline_name
    finally:
        PipelineCatalogArrowEndpoints(arrow_client).drop(pipeline_name)
