from unittest import mock

import pandas as pd
import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
from graphdatascience.procedure_surface.api.pipeline import PipelineCatalogEntry
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
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


def test_node_classification_train_runs_arrow_job_and_returns_arrow_wired_model() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_classification_train_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.node_classification_train_arrow_endpoints.JobClient.get_summary",
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


def test_node_classification_train_estimate_runs_arrow_job() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.node_property_endpoints.NodePropertyEndpointsHelper.estimate",
            return_value=mock.Mock(node_count=4),
        ) as estimate,
    ):
        result = NodeClassificationPipelineArrowEndpoints(arrow_client, None).train.estimate(
            graph,
            "pipe",
            metrics=["F1_WEIGHTED"],
            model_name="model",
            target_property="y",
        )

    assert result.node_count == 4
    estimate.assert_called_once()


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


def test_node_classification_get_uses_shared_pipeline_catalog() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    pipeline_catalog = mock.Mock(spec=PipelineCatalogProtocol)
    pipeline_catalog.exists.return_value = PipelineCatalogEntry(
        pipelineName="pipe", pipelineType="Node classification training pipeline"
    )

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints.PipelineCatalogArrowEndpoints",
        return_value=pipeline_catalog,
    ) as pipeline_catalog_cls:
        pipeline = NodeClassificationPipelineArrowEndpoints(
            arrow_client,
            None,
        ).get("pipe")

    assert pipeline.name() == "pipe"
    pipeline_catalog_cls.assert_called_once_with(
        arrow_client,
        None,
        show_progress=True,
    )
    pipeline_catalog.exists.assert_called_once_with("pipe")
    arrow_client.do_action_with_retry.assert_not_called()


def test_node_classification_endpoints_do_not_accept_pipeline_catalog_constructor_override() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    constructor = mock.Mock(side_effect=NodeClassificationPipelineArrowEndpoints)

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints.PipelineCatalogArrowEndpoints"
    ):
        with pytest.raises(TypeError, match="pipeline_catalog"):
            constructor(arrow_client, None, pipeline_catalog=mock.Mock())


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
