from unittest import mock

import pandas as pd
from pyarrow import flight

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.model.node_regression_model import NodeRegressionModelV2
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


def test_node_regression_create_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureProperties":[]}')]

    pipeline, result = NodeRegressionPipelineArrowEndpoints(arrow_client, None).create("pipe")

    assert pipeline.name() == "pipe"
    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeRegression",
        {"pipelineName": "pipe"},
    )


def test_node_regression_add_random_forest_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureProperties":[]}')]

    result = NodeRegressionPipelineArrowEndpoints(arrow_client, None).add_random_forest(
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


def test_node_regression_add_random_forest_accepts_list_range_inputs() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureProperties":[]}')]

    NodeRegressionPipelineArrowEndpoints(arrow_client, None).add_random_forest(
        "pipe",
        max_depth=[3, 9],
        number_of_decision_trees=[10, 50],
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


def test_node_regression_add_linear_regression_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureProperties":[]}')]

    result = NodeRegressionPipelineArrowEndpoints(arrow_client, None).add_linear_regression(
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
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureProperties":[]}')]

    result = NodeRegressionPipelineArrowEndpoints(arrow_client, None).add_node_property(
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


def test_node_regression_get_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [
        _flight_result('{"pipelineName":"pipe","pipelineType":"Node regression training pipeline"}')
    ]

    pipeline = NodeRegressionPipelineArrowEndpoints(arrow_client, None).get("pipe")

    assert pipeline.name() == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.list",
        {"pipelineName": "pipe"},
    )


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
            return_value={"trainMillis": 7, "modelInfo": {"modelName": "model"}, "modelSelectionStats": {}},
        ) as get_summary,
    ):
        endpoints = NodeRegressionPipelineArrowEndpoints(arrow_client, None)
        model, result = endpoints.train(graph, "pipe", metrics=["MAE"], model_name="model", target_property="y")

        assert isinstance(model, NodeRegressionModelV2)
        assert model.name() == "model"
        assert result.train_millis == 7
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


def test_node_regression_predict_mutate_runs_arrow_mutation() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    mutate_response = mock.Mock()
    mutate_response.node_properties_written = 4
    mutate_response.mutate_millis = 5

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints.JobClient.get_summary",
            return_value={"computeMillis": 1, "postProcessingMillis": 2, "preProcessingMillis": 3, "configuration": {}},
        ),
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints.MutationClient.mutate_node_property",
            return_value=mutate_response,
        ),
    ):
        result = NodeRegressionPredictArrowEndpoints(arrow_client, None).mutate(
            graph,
            model_name="model",
            mutate_property="predicted",
        )

    assert result.node_properties_written == 4
    assert result.mutate_millis == 5
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
