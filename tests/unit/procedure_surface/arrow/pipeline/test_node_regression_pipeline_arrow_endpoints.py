import json
from typing import Any, cast
from unittest import mock

import pandas as pd
import pytest
from pyarrow import flight

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.model.node_regression_model import NodeRegressionModelV2
from graphdatascience.procedure_surface.api.pipeline import PipelineCatalogEntry
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints import (
    NodeRegressionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints import (
    NodeRegressionPredictArrowEndpoints,
)


def _flight_result(payload: str) -> flight.Result:
    body = mock.Mock()
    body.to_pybytes.return_value = payload.encode("utf-8")
    return mock.Mock(body=body, spec=flight.Result)


def _info_payload(**overrides: object) -> str:
    payload: dict[str, object] = {
        "autoTuningConfig": {},
        "featureProperties": [],
        "name": "pipe",
        "nodePropertySteps": [],
        "parameterSpace": {},
        "splitConfig": {},
    }
    payload.update(overrides)
    return json.dumps(payload)


def _train_summary(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "configuration": {},
        "modelInfo": {
            "bestParameters": {},
            "metrics": {},
            "modelName": "model",
            "modelType": "NodeRegression",
            "pipeline": {"nodePropertySteps": []},
        },
        "modelSelectionStats": {},
        "trainMillis": 7,
    }
    payload.update(overrides)
    return payload


def test_node_regression_create_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result(_info_payload())]

    pipeline, result = NodeRegressionPipelineArrowEndpoints(arrow_client).create("pipe")

    assert pipeline.name() == "pipe"
    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeRegression",
        {"pipelineName": "pipe"},
    )


def test_node_regression_add_random_forest_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result(_info_payload())]

    result = NodeRegressionPipelineArrowEndpoints(arrow_client).add_random_forest(
        "pipe",
        number_of_decision_trees=42,
        max_depth=9,
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeRegression.modelCandidate.add",
        {
            "pipelineName": "pipe",
            "modelType": "RandomForest",
            "maxDepth": 9,
            "minLeafSize": 1,
            "minSplitSize": 2,
            "numberOfDecisionTrees": 42,
            "numberOfSamplesRatio": 1.0,
        },
    )


def test_node_regression_add_random_forest_accepts_tuple_range_inputs() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result(_info_payload())]

    NodeRegressionPipelineArrowEndpoints(arrow_client).add_random_forest(
        "pipe",
        max_depth=(3, 9),
        number_of_decision_trees=(10, 50),
    )

    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeRegression.modelCandidate.add",
        {
            "pipelineName": "pipe",
            "modelType": "RandomForest",
            "maxDepth": {"range": [3, 9]},
            "minLeafSize": 1,
            "minSplitSize": 2,
            "numberOfDecisionTrees": {"range": [10, 50]},
            "numberOfSamplesRatio": 1.0,
        },
    )


def test_node_regression_add_random_forest_rejects_list_range_inputs() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)

    with pytest.raises(ValueError, match="max_depth range inputs must be tuples with exactly two values."):
        NodeRegressionPipelineArrowEndpoints(arrow_client).add_random_forest(
            "pipe",
            max_depth=cast(Any, [3, 9]),
        )


def test_node_regression_add_linear_regression_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result(_info_payload())]

    result = NodeRegressionPipelineArrowEndpoints(arrow_client).add_linear_regression(
        "pipe",
        max_epochs=3,
        min_epochs=1,
        penalty=1.0,
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeRegression.modelCandidate.add",
        {
            "pipelineName": "pipe",
            "modelType": "LinearRegression",
            "batchSize": 100,
            "learningRate": 0.001,
            "maxEpochs": 3,
            "minEpochs": 1,
            "patience": 1,
            "penalty": 1.0,
            "tolerance": 0.001,
        },
    )


def test_node_regression_add_node_property_runs_arrow_action_with_config() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result(_info_payload())]

    result = NodeRegressionPipelineArrowEndpoints(arrow_client).add_node_property(
        "pipe",
        "pageRank",
        mutate_property="pr",
        max_iterations=10,
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeRegression.nodeProperty.add",
        {
            "pipelineName": "pipe",
            "procedureName": "pageRank",
            "procedureConfiguration": {
                "mutateProperty": "pr",
                "maxIterations": 10,
            },
        },
    )


def test_node_regression_get_uses_shared_pipeline_catalog() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    pipeline_catalog = mock.Mock(spec=PipelineCatalogProtocol)
    pipeline_catalog.exists.return_value = PipelineCatalogEntry(
        pipelineName="pipe", pipelineType="Node regression training pipeline"
    )

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints.PipelineCatalogArrowEndpoints",
        return_value=pipeline_catalog,
    ) as pipeline_catalog_cls:
        pipeline = NodeRegressionPipelineArrowEndpoints(
            arrow_client,
        ).get("pipe")

    assert pipeline.name() == "pipe"
    pipeline_catalog_cls.assert_called_once_with(
        arrow_client,
        show_progress=True,
    )
    pipeline_catalog.exists.assert_called_once_with("pipe")
    arrow_client.do_action_with_retry.assert_not_called()


def test_node_regression_endpoints_do_not_accept_pipeline_catalog_constructor_override() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    constructor = mock.Mock(side_effect=NodeRegressionPipelineArrowEndpoints)

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints.PipelineCatalogArrowEndpoints"
    ):
        with pytest.raises(TypeError, match="pipeline_catalog"):
            constructor(arrow_client, None, pipeline_catalog=mock.Mock())


def test_node_regression_train_runs_arrow_job_and_returns_arrow_wired_model() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints.JobClient.get_summary",
            return_value=_train_summary(),
        ) as get_summary,
    ):
        endpoints = NodeRegressionPipelineArrowEndpoints(arrow_client)
        model, result = endpoints.train(graph, "pipe", metrics=["MAE"], model_name="model", target_property="y")

        assert isinstance(model, NodeRegressionModelV2)
        assert model.name() == "model"
        assert result.train_millis == 7
        assert result.model_info is not None
        assert result.model_info.model_name == "model"
        run_job_and_wait.assert_called_once()
        get_summary.assert_called_once_with(arrow_client, "job-1")


def test_node_regression_predict_stream_runs_arrow_job() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"
    expected = pd.DataFrame({"nodeId": [0], "predictedValue": [1.0]})

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints.JobClient.stream_results",
            return_value=expected,
        ) as stream_results,
    ):
        result = NodeRegressionPredictArrowEndpoints(arrow_client, None).stream(graph, model_name="model")

    assert result.equals(expected)
    run_job_and_wait.assert_called_once_with(
        arrow_client,
        "v2/pipeline.nodeRegression.predict",
        {
            "graphName": "g",
            "modelName": "model",
            "logProgress": True,
            "sudo": False,
        },
        show_progress=True,
    )
    stream_results.assert_called_once_with(arrow_client, "g", "job-1")
