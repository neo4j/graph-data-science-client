import json
from typing import Generator
from uuid import uuid4

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints import (
    NodeClassificationPipelineArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph, create_graph_from_db

graph = """
    CREATE
    (a: Node {feature: 1.0, target: 0}),
    (b: Node {feature: 2.0, target: 1}),
    (c: Node {feature: 3.0, target: 0}),
    (d: Node {feature: 4.0, target: 1}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
"""


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, f"node-classification-g-{uuid4().hex[:8]}", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                        MATCH (n)-->(m)
                        WITH gds.graph.project.remote(n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) as g
                        RETURN g
                    """,
    ) as g:
        yield g


@pytest.fixture
def endpoints(arrow_client: AuthenticatedArrowClient) -> NodeClassificationPipelineArrowEndpoints:
    return NodeClassificationPipelineArrowEndpoints(arrow_client, None, show_progress=False)


def _drop_pipeline(arrow_client: AuthenticatedArrowClient, pipeline_name: str) -> None:
    arrow_client.do_action_with_retry(
        "v2/pipeline.drop",
        {"pipelineName": pipeline_name, "failIfMissing": False},
    )


@pytest.mark.db_integration
def test_node_classification_train_and_predict_write(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    db_graph: GraphV2,
) -> None:
    endpoints = NodeClassificationPipelineArrowEndpoints(
        arrow_client, RemoteWriteBackClient(arrow_client, query_runner), show_progress=False
    )

    pipeline_name = f"nc-pipe-{uuid4().hex[:8]}"
    model_name = f"nc-model-{uuid4().hex[:8]}"

    try:
        pipeline, create_result = endpoints.create(pipeline_name)
        pipeline.select_features(node_properties=["feature"])
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, train_result = pipeline.train(
            db_graph,
            metrics=["F1_WEIGHTED"],
            model_name=model_name,
            target_property="target",
        )

        assert train_result.train_millis is not None
        assert train_result.train_millis >= 0
        assert model.exists()

        write_result = model.predict_write(
            db_graph,
            write_property="myPredictedClass",
            predicted_probability_property="myPredictedProbabilities",
        )

        assert write_result.node_properties_written == 8
        assert write_result.write_millis is not None
        assert write_result.write_millis >= 0
    finally:
        arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model_name}).encode("utf-8"))
        _drop_pipeline(arrow_client, pipeline_name)


def test_node_classification_train_and_predict_and_stream(
    arrow_client: AuthenticatedArrowClient,
    endpoints: NodeClassificationPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nc-pipe-{uuid4().hex[:8]}"
    model_name = f"nc-model-{uuid4().hex[:8]}"

    try:
        pipeline, create_result = endpoints.create(pipeline_name)
        pipeline.select_features(node_properties=["feature"])
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, train_result = pipeline.train(
            sample_graph,
            metrics=["F1_WEIGHTED"],
            model_name=model_name,
            target_property="target",
        )

        stream_result = model.predict_stream(sample_graph)

        assert "predictedClass" in stream_result.columns
        assert "predictedProbabilities" in stream_result.columns
        assert len(stream_result) == 4
    finally:
        arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model_name}).encode("utf-8"))
        _drop_pipeline(arrow_client, pipeline_name)


def test_node_classification_predict_mutate(
    arrow_client: AuthenticatedArrowClient,
    endpoints: NodeClassificationPipelineArrowEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nc-pipe-{uuid4().hex[:8]}"
    model_name = f"nc-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.select_features(node_properties=["feature"])
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, _ = pipeline.train(
            sample_graph,
            metrics=["F1_WEIGHTED"],
            model_name=model_name,
            target_property="target",
        )

        mutate_result = model.predict_mutate(
            sample_graph,
            mutate_property="predictedClass",
            predicted_probability_property="predictedProbabilities",
        )

        assert mutate_result.node_properties_written == 8
        assert mutate_result.mutate_millis is not None
        assert mutate_result.mutate_millis >= 0
        assert mutate_result.configuration and mutate_result.configuration["mutateProperty"] == "predictedClass"
    finally:
        arrow_client.do_action_with_retry("v2/model.drop", json.dumps({"modelName": model_name}).encode("utf-8"))
        _drop_pipeline(arrow_client, pipeline_name)
