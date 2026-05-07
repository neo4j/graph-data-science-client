import json
from typing import Generator
from uuid import uuid4

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints import (
    NodeRegressionPipelineArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    gdl = """
    CREATE
    (a: Node {feature: 1.0, target: 1.0}),
    (b: Node {feature: 2.0, target: 2.0}),
    (c: Node {feature: 3.0, target: 3.0}),
    (d: Node {feature: 4.0, target: 4.0}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """

    with create_graph(arrow_client, f"node-regression-g-{uuid4().hex[:8]}", gdl) as G:
        yield G


@pytest.fixture
def endpoints(arrow_client: AuthenticatedArrowClient) -> NodeRegressionPipelineArrowEndpoints:
    return NodeRegressionPipelineArrowEndpoints(arrow_client, None, show_progress=False)


def _drop_pipeline(arrow_client: AuthenticatedArrowClient, pipeline_name: str) -> None:
    arrow_client.do_action_with_retry(
        "v2/pipeline.drop",
        {"pipelineName": pipeline_name, "failIfMissing": False},
    )


def test_node_regression_train_and_predict_stream(
    arrow_client: AuthenticatedArrowClient,
    endpoints: NodeRegressionPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nr-pipe-{uuid4().hex[:8]}"
    model_name = f"nr-model-{uuid4().hex[:8]}"

    try:
        pipeline, create_result = endpoints.create(pipeline_name)
        node_property_result = pipeline.add_node_property("pageRank", mutate_property="pr")
        feature_result = pipeline.select_features(node_properties=["pr"])
        regression_result = pipeline.add_linear_regression(max_epochs=1, min_epochs=1)
        model, train_result = pipeline.train(
            sample_graph,
            metrics=["MEAN_ABSOLUTE_ERROR"],
            model_name=model_name,
            target_property="target",
        )
        stream_result = model.predict_stream(sample_graph)

        assert create_result.name == pipeline_name
        assert node_property_result.name == pipeline_name
        assert feature_result.name == pipeline_name
        assert regression_result.name == pipeline_name
        assert train_result.train_millis is not None
        assert train_result.train_millis >= 0
        assert model.exists()
        assert "predictedValue" in stream_result.columns
        assert len(stream_result) == 4
    finally:
        arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model_name}).encode("utf-8"))
        _drop_pipeline(arrow_client, pipeline_name)


def test_node_regression_predict_mutate(
    arrow_client: AuthenticatedArrowClient,
    endpoints: NodeRegressionPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nr-pipe-{uuid4().hex[:8]}"
    model_name = f"nr-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.select_features(node_properties=["feature"])
        pipeline.add_linear_regression(max_epochs=1, min_epochs=1)
        model, _ = pipeline.train(
            sample_graph,
            metrics=["MEAN_ABSOLUTE_ERROR"],
            model_name=model_name,
            target_property="target",
        )

        mutate_result = model.predict_mutate(sample_graph, mutate_property="predicted")

        assert mutate_result.node_properties_written == 4
        assert mutate_result.mutate_millis is not None
        assert mutate_result.mutate_millis >= 0
    finally:
        arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model_name}).encode("utf-8"))
        _drop_pipeline(arrow_client, pipeline_name)


def test_node_regression_get_returns_pipeline_object(
    arrow_client: AuthenticatedArrowClient,
    endpoints: NodeRegressionPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nr-pipe-{uuid4().hex[:8]}"

    try:
        pipeline, create_result = endpoints.create(pipeline_name)
        pipeline.select_features(node_properties=["feature"])
        pipeline.add_linear_regression(max_epochs=1, min_epochs=1)
        fetched_pipeline = endpoints.get(pipeline_name)

        assert fetched_pipeline.name() == pipeline_name
    finally:
        _drop_pipeline(arrow_client, pipeline_name)
