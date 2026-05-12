from typing import Any, cast
from unittest import mock

import pandas as pd
import pytest
from pyarrow import flight

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModelV2
from graphdatascience.procedure_surface.api.pipeline import PipelineCatalogEntry
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.arrow.pipeline.link_prediction_pipeline_arrow_endpoints import (
    LinkPredictionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.link_prediction_predict_arrow_endpoints import (
    LinkPredictionPredictArrowEndpoints,
)


def _flight_result(payload: str) -> flight.Result:
    body = mock.Mock()
    body.to_pybytes.return_value = payload.encode("utf-8")
    return mock.Mock(body=body, spec=flight.Result)


def test_link_prediction_create_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureSteps":[]}')]

    pipeline, result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).create("pipe")

    assert pipeline.name() == "pipe"
    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.linkPrediction",
        {"pipelineName": "pipe"},
    )


def test_link_prediction_add_feature_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureSteps":[]}')]

    result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).add_feature(
        "pipe",
        "L2",
        node_properties=["embedding"],
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.linkPrediction.feature.add",
        {
            "pipelineName": "pipe",
            "featureType": "L2",
            "nodeProperties": ["embedding"],
        },
    )


def test_link_prediction_configure_split_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","splitConfig":{}}')]

    result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).configure_split(
        "pipe",
        negative_relationship_type="NO_REL",
        negative_sampling_ratio=2.5,
        test_fraction=0.2,
        train_fraction=0.7,
        validation_folds=5,
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.linkPrediction.split.configure",
        {
            "pipelineName": "pipe",
            "negativeRelationshipType": "NO_REL",
            "negativeSamplingRatio": 2.5,
            "testFraction": 0.2,
            "trainFraction": 0.7,
            "validationFolds": 5,
        },
    )


def test_link_prediction_add_mlp_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureSteps":[]}')]

    result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).add_mlp(
        "pipe",
        batch_size=256,
        class_weights=[0.2, 0.8],
        focus_weight=0.3,
        hidden_layer_sizes=[64, 16],
        learning_rate=0.01,
        max_epochs=12,
        min_epochs=2,
        patience=4,
        penalty=0.1,
        tolerance=0.0002,
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.linkPrediction.modelCandidate.add",
        {
            "pipelineName": "pipe",
            "modelType": "MLP",
            "batchSize": 256,
            "classWeights": [0.2, 0.8],
            "focusWeight": 0.3,
            "hiddenLayerSizes": [64, 16],
            "learningRate": 0.01,
            "maxEpochs": 12,
            "minEpochs": 2,
            "patience": 4,
            "penalty": 0.1,
            "tolerance": 0.0002,
        },
    )


def test_link_prediction_add_mlp_uses_default_hidden_layer_sizes() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureSteps":[]}')]

    result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).add_mlp("pipe")

    assert result.name == "pipe"
    assert arrow_client.do_action_with_retry.call_args.args[1]["hiddenLayerSizes"] == [100]


def test_link_prediction_add_random_forest_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","featureSteps":[]}')]

    result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).add_random_forest(
        "pipe",
        criterion="ENTROPY",
        max_depth=8,
        max_features_ratio=0.4,
        min_leaf_size=3,
        min_split_size=6,
        number_of_decision_trees=40,
        number_of_samples_ratio=0.9,
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.linkPrediction.modelCandidate.add",
        {
            "pipelineName": "pipe",
            "modelType": "RandomForest",
            "criterion": "ENTROPY",
            "maxDepth": 8,
            "maxFeaturesRatio": 0.4,
            "minLeafSize": 3,
            "minSplitSize": 6,
            "numberOfDecisionTrees": 40,
            "numberOfSamplesRatio": 0.9,
        },
    )


def test_link_prediction_add_node_property_runs_arrow_action_with_config() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [_flight_result('{"name":"pipe","nodePropertySteps":[]}')]

    result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).add_node_property(
        "pipe",
        "degree",
        mutate_property="pr",
        max_iterations=10,
    )

    assert result.name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.linkPrediction.nodeProperty.add",
        {
            "pipelineName": "pipe",
            "procedureName": "degree",
            "procedureConfiguration": {
                "mutateProperty": "pr",
                "maxIterations": 10,
            },
        },
    )


def test_link_prediction_configure_auto_tuning_runs_arrow_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = [
        _flight_result('{"name":"pipe","autoTuningConfig":{"maxTrials":42}}')
    ]

    result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).configure_auto_tuning("pipe", max_trials=42)

    assert result.name == "pipe"
    assert result.auto_tuning_config == {"maxTrials": 42}
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.linkPrediction.autoTuning.configure",
        {"pipelineName": "pipe", "maxTrials": 42},
    )


def test_link_prediction_get_uses_shared_pipeline_catalog() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    pipeline_catalog = mock.Mock(spec=PipelineCatalogProtocol)
    pipeline_catalog.exists.return_value = PipelineCatalogEntry(
        pipelineName="pipe", pipelineType="Link prediction training pipeline"
    )

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.pipeline.link_prediction_pipeline_arrow_endpoints.PipelineCatalogArrowEndpoints",
        return_value=pipeline_catalog,
    ):
        pipeline = LinkPredictionPipelineArrowEndpoints(arrow_client, None).get("pipe")

    assert pipeline.name() == "pipe"
    pipeline_catalog.exists.assert_called_once_with("pipe")


def test_link_prediction_train_runs_arrow_job_and_returns_arrow_wired_model() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.link_prediction_train_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.link_prediction_train_arrow_endpoints.JobClient.get_summary",
            return_value={"trainMillis": 7, "modelInfo": {"modelName": "model"}, "modelSelectionStats": {}},
        ),
    ):
        endpoints = LinkPredictionPipelineArrowEndpoints(arrow_client, None)
        model, result = endpoints.train(
            graph,
            "pipe",
            model_name="model",
            negative_class_weight=2.5,
            target_relationship_type="REL",
        )

    assert isinstance(model, LinkPredictionModelV2)
    assert model.name() == "model"
    assert result.train_millis == 7
    assert run_job_and_wait.call_args.args[2]["negativeClassWeight"] == 2.5


def test_link_prediction_train_estimate_runs_arrow_estimate_helper() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.node_property_endpoints.NodePropertyEndpointsHelper.estimate",
        return_value=mock.Mock(node_count=4),
    ) as estimate:
        result = LinkPredictionPipelineArrowEndpoints(arrow_client, None).train.estimate(
            graph,
            "pipe",
            model_name="model",
            negative_class_weight=2.5,
            target_relationship_type="REL",
        )

    assert result.node_count == 4
    estimate.assert_called_once()
    assert estimate.call_args.kwargs["algo_config"]["negativeClassWeight"] == 2.5


def test_link_prediction_predict_stream_runs_arrow_job() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"
    expected = pd.DataFrame({"sourceNodeId": [0], "targetNodeId": [1], "probability": [0.9]})

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.link_prediction_predict_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.pipeline.link_prediction_predict_arrow_endpoints.JobClient.stream_results",
            return_value=expected,
        ),
    ):
        result = LinkPredictionPredictArrowEndpoints(arrow_client, None).stream(
            graph,
            model_name="model",
            relationship_types=["REL"],
            sample_rate=0.7,
            threshold=0.5,
            top_k=3,
            top_n=2,
            initial_sampler="UNIFORM",
            delta_threshold=0.01,
            max_iterations=10,
            random_joins=4,
            random_seed=42,
        )

    assert result.equals(expected)
    run_job_and_wait.assert_called_once_with(
        arrow_client,
        "v2/pipeline.linkPrediction.predict",
        {
            "graphName": "g",
            "modelName": "model",
            "relationshipTypes": ["REL"],
            "sampleRate": 0.7,
            "threshold": 0.5,
            "topK": 3,
            "topN": 2,
            "initialSampler": "UNIFORM",
            "deltaThreshold": 0.01,
            "maxIterations": 10,
            "randomJoins": 4,
            "randomSeed": 42,
            "logProgress": True,
            "sudo": False,
        },
        show_progress=True,
    )


def test_link_prediction_predict_estimate_runs_arrow_estimate_helper() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    graph = mock.Mock()
    graph.name.return_value = "g"

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.relationship_endpoints_helper.RelationshipEndpointsHelper.estimate",
        return_value=mock.Mock(node_count=4),
    ) as estimate:
        result = LinkPredictionPredictArrowEndpoints(arrow_client, None).estimate(graph, model_name="model", top_n=2)

    assert result.node_count == 4
    estimate.assert_called_once()
    assert estimate.call_args.args[2]["topN"] == 2


def test_link_prediction_predict_stream_rejects_target_relationship_type() -> None:
    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, LinkPredictionPredictArrowEndpoints(mock.Mock(spec=AuthenticatedArrowClient), None).stream)(
            mock.Mock(),
            model_name="model",
            target_relationship_type="REL",
        )


def test_link_prediction_predict_estimate_rejects_target_relationship_type() -> None:
    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, LinkPredictionPredictArrowEndpoints(mock.Mock(spec=AuthenticatedArrowClient), None).estimate)(
            mock.Mock(),
            model_name="model",
            target_relationship_type="REL",
        )


def test_link_prediction_predict_mutate_passes_top_n() -> None:
    graph = mock.Mock()
    endpoints = LinkPredictionPredictArrowEndpoints(mock.Mock(spec=AuthenticatedArrowClient), None)

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.relationship_endpoints_helper.RelationshipEndpointsHelper.run_job_and_mutate",
        return_value={
            "relationshipsWritten": 1,
            "probabilityDistribution": {"min": 0.1, "max": 0.9},
            "samplingStats": {"positive": 3, "negative": 6},
        },
    ) as run_job_and_mutate:
        result = endpoints.mutate(
            graph,
            model_name="model",
            mutate_relationship_type="PREDICTED",
            mutate_property="score",
            relationship_types=["REL"],
            sample_rate=0.7,
            threshold=0.5,
            top_k=3,
            top_n=2,
            initial_sampler="UNIFORM",
            delta_threshold=0.01,
            max_iterations=10,
            random_joins=4,
            random_seed=42,
        )

    run_job_and_mutate.assert_called_once_with(
        "v2/pipeline.linkPrediction.predict",
        mock.ANY,
        mutate_property="score",
        mutate_relationship_type="PREDICTED",
    )
    config = run_job_and_mutate.call_args.args[1]
    assert config["relationshipTypes"] == ["REL"]
    assert config["sampleRate"] == 0.7
    assert config["threshold"] == 0.5
    assert config["topK"] == 3
    assert run_job_and_mutate.call_args.args[1]["topN"] == 2
    assert config["initialSampler"] == "UNIFORM"
    assert config["deltaThreshold"] == 0.01
    assert config["maxIterations"] == 10
    assert config["randomJoins"] == 4
    assert config["randomSeed"] == 42
    assert result.probability_distribution == {"min": 0.1, "max": 0.9}
    assert result.sampling_stats == {"positive": 3, "negative": 6}
