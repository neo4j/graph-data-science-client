from unittest import mock

import pandas as pd

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints import (
    NodeClassificationPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_predict_arrow_endpoints import (
    NodeClassificationPredictArrowEndpoints,
)


def test_node_classification_predict_stream_forces_probability_distribution() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"
    expected = pd.DataFrame({"nodeId": [0], "predictedClass": [1], "predictedProbabilities": [[0.2, 0.8]]})

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_classification_predict_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_classification_predict_arrow_endpoints.JobClient.stream_results",
            return_value=expected,
        ) as stream_results,
    ):
        result = NodeClassificationPredictArrowEndpoints(arrow_client, None).stream(
            graph,
            model_name="model",
        )

    assert result.equals(expected)
    run_job_and_wait.assert_called_once_with(
        arrow_client,
        "v2/pipeline.nodeClassification.predict",
        {
            "graphName": "g",
            "modelName": "model",
            "includePredictedProbabilities": True,
            "logProgress": True,
            "sudo": False,
        },
        show_progress=True,
    )
    stream_results.assert_called_once_with(arrow_client, "g", "job-1")


def test_node_classification_predict_write_uses_node_property_helper() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    helper_result = {
        "computeMillis": 1,
        "writeMillis": 5,
        "nodePropertiesWritten": 4,
        "postProcessingMillis": 2,
        "preProcessingMillis": 3,
        "configuration": {"writeProperty": "predicted"},
    }

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.node_property_endpoints.NodePropertyEndpointsHelper.run_job_and_write",
        return_value=helper_result,
    ) as run_job_and_write:
        result = NodeClassificationPredictArrowEndpoints(arrow_client, None).write(
            graph,
            model_name="model",
            write_property="predicted",
            predicted_probability_property="probabilities",
        )

    assert result.node_properties_written == 4
    assert result.write_millis == 5
    run_job_and_write.assert_called_once_with(
        "v2/pipeline.nodeClassification.predict",
        graph,
        {
            "graphName": "g",
            "modelName": "model",
            "logProgress": True,
            "sudo": False,
        },
        {
            "writeProperty": "predicted",
            "predictedProbabilityProperty": "probabilities",
        },
        write_concurrency=None,
        concurrency=None,
    )


def test_node_classification_train_runs_arrow_job_and_returns_arrow_wired_model() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints.JobClient.get_summary",
            return_value={"trainMillis": 7, "modelInfo": {"modelName": "model"}, "modelSelectionStats": {}},
        ) as get_summary,
    ):
        endpoints = NodeClassificationPipelineArrowEndpoints(arrow_client, None)
        model, result = endpoints.train(graph, "pipe", metrics=["F1_WEIGHTED"], model_name="model", target_property="y")

    assert isinstance(model, NodeClassificationModelV2)
    assert model.name() == "model"
    assert result.train_millis == 7
    assert result.model_info is not None
    assert result.model_info.model_name == "model"
    run_job_and_wait.assert_called_once()
    get_summary.assert_called_once_with(arrow_client, "job-1")


def test_node_classification_select_features_uses_node_properties_payload() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    row = mock.Mock()
    row.body.to_pybytes.return_value = b'{"name":"pipe","featureProperties":[]}'
    arrow_client.do_action_with_retry.return_value = [row]

    result = NodeClassificationPipelineArrowEndpoints(arrow_client, None).select_features(
        "pipe",
        node_properties=["feature"],
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeClassification.features.select",
        {
            "pipelineName": "pipe",
            "nodeProperties": ["feature"],
        },
    )


def test_node_classification_add_mlp_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    row = mock.Mock()
    row.body.to_pybytes.return_value = b'{"name":"pipe","featureProperties":[]}'
    arrow_client.do_action_with_retry.return_value = [row]

    result = NodeClassificationPipelineArrowEndpoints(arrow_client, None).add_mlp(
        "pipe", hidden_layer_sizes=[64, 16, 4], penalty=0.1
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.nodeClassification.modelCandidate.add",
        {
            "pipelineName": "pipe",
            "modelType": "MLP",
            "hiddenLayerSizes": [64, 16, 4],
            "penalty": 0.1,
        },
    )
