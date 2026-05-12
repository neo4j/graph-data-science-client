from unittest import mock

import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
from graphdatascience.procedure_surface.api.pipeline import PipelineCatalogEntry
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline import (
    NodeClassificationPipeline,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints import (
    NodeClassificationPipelineCypherEndpoints,
)


def _info_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "autoTuningConfig": {},
        "featureProperties": [],
        "name": "pipe",
        "nodePropertySteps": [],
        "parameterSpace": {},
        "splitConfig": {},
    }
    payload.update(overrides)
    return payload


def _train_summary(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "configuration": {},
        "modelInfo": {
            "bestParameters": {},
            "metrics": {},
            "modelName": "model",
            "modelType": "NodeClassification",
            "pipeline": {"nodePropertySteps": []},
        },
        "modelSelectionStats": {},
        "trainMillis": 7,
    }
    payload.update(overrides)
    return payload


def test_node_classification_train_runs_query() -> None:
    create_row = mock.Mock()
    create_row.to_dict.return_value = _info_payload()
    row = mock.Mock()
    row.to_dict.return_value = _train_summary()
    query_runner = mock.Mock()
    query_runner.call_procedure.side_effect = [
        mock.Mock(squeeze=mock.Mock(return_value=create_row)),
        mock.Mock(squeeze=mock.Mock(return_value=row)),
    ]
    graph = mock.Mock()
    graph.name.return_value = "g"

    pipeline, _ = NodeClassificationPipelineCypherEndpoints(query_runner).create("pipe")
    model, result = pipeline.train(graph, metrics=["F1_WEIGHTED"], model_name="model", target_property="y")

    assert isinstance(model, NodeClassificationModelV2)
    assert model.name() == "model"
    assert result.train_millis == 7
    assert result.model_info is not None
    assert result.model_info.model_name == "model"
    assert (
        query_runner.call_procedure.call_args_list[1].kwargs["endpoint"] == "gds.beta.pipeline.nodeClassification.train"
    )


def test_node_classification_train_estimate_runs_query() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {
        "nodeCount": 4,
        "relationshipCount": 4,
        "requiredMemory": "42 KiB",
        "treeView": "",
        "mapView": {},
        "bytesMin": 1,
        "bytesMax": 2,
        "heapPercentageMin": 0.1,
        "heapPercentageMax": 0.2,
    }
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))
    graph = mock.Mock()
    graph.name.return_value = "g"

    result = NodeClassificationPipelineCypherEndpoints(query_runner).train.estimate(
        graph,
        "pipe",
        metrics=["F1_WEIGHTED"],
        model_name="model",
        target_property="y",
    )

    assert result.node_count == 4
    assert (
        query_runner.call_procedure.call_args.kwargs["endpoint"]
        == "gds.beta.pipeline.nodeClassification.train.estimate"
    )


def test_node_classification_pipeline_train_estimate_delegates_to_trainer() -> None:
    trainer = mock.Mock()
    trainer.train = mock.Mock()
    graph = mock.Mock()
    expected = mock.Mock()
    trainer.train.estimate.return_value = expected

    pipeline = NodeClassificationPipeline("pipe", mock.Mock(), trainer, mock.Mock())

    result = pipeline.train_estimate(
        graph,
        metrics=["F1_WEIGHTED"],
        model_name="model",
        target_property="y",
    )

    assert result is expected
    trainer.train.estimate.assert_called_once_with(
        graph,
        "pipe",
        metrics=["F1_WEIGHTED"],
        model_name="model",
        target_property="y",
        relationship_types=["*"],
        target_node_labels=["*"],
        store_model_to_disk=False,
        random_seed=None,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=None,
        job_id=None,
    )


def test_node_classification_create_returns_info_result() -> None:
    row = mock.Mock()
    row.to_dict.return_value = _info_payload()
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    pipeline, result = NodeClassificationPipelineCypherEndpoints(query_runner).create("pipe")

    assert pipeline.name() == "pipe"
    assert type(result) is NodeClassificationPipelineInfoResult
    assert result.name == "pipe"


def test_node_classification_get_uses_shared_pipeline_catalog() -> None:
    query_runner = mock.Mock()
    pipeline_catalog = mock.Mock(spec=PipelineCatalogProtocol)
    pipeline_catalog.exists.return_value = PipelineCatalogEntry(
        pipelineName="pipe", pipelineType="Node classification training pipeline"
    )

    with mock.patch(
        "graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints.PipelineCatalogCypherEndpoints",
        return_value=pipeline_catalog,
    ) as pipeline_catalog_cls:
        pipeline = NodeClassificationPipelineCypherEndpoints(query_runner).get("pipe")

    assert pipeline.name() == "pipe"
    pipeline_catalog_cls.assert_called_once_with(query_runner)
    pipeline_catalog.exists.assert_called_once_with("pipe")
    query_runner.call_procedure.assert_not_called()


def test_node_classification_endpoints_do_not_accept_pipeline_catalog_constructor_override() -> None:
    query_runner = mock.Mock()
    constructor = mock.Mock(side_effect=NodeClassificationPipelineCypherEndpoints)

    with mock.patch(
        "graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints.PipelineCatalogCypherEndpoints"
    ):
        with pytest.raises(TypeError, match="pipeline_catalog"):
            constructor(query_runner, pipeline_catalog=mock.Mock())


def test_node_classification_predict_stream_mutate_and_write_run_queries() -> None:
    mutate_row = mock.Mock()
    mutate_row.to_dict.return_value = {
        "computeMillis": 1,
        "mutateMillis": 3,
        "nodePropertiesWritten": 4,
        "postProcessingMillis": 5,
        "preProcessingMillis": 2,
        "configuration": {},
    }
    write_row = mock.Mock()
    write_row.to_dict.return_value = {
        "computeMillis": 1,
        "writeMillis": 4,
        "nodePropertiesWritten": 8,
        "postProcessingMillis": 5,
        "preProcessingMillis": 2,
        "configuration": {},
    }
    query_runner = mock.Mock()
    query_runner.call_procedure.side_effect = [
        pd.DataFrame({"nodeId": [0], "predictedClass": [1], "predictedProbabilities": [[0.1, 0.9]]}),
        mock.Mock(squeeze=mock.Mock(return_value=mutate_row)),
        mock.Mock(squeeze=mock.Mock(return_value=write_row)),
    ]
    graph = mock.Mock()
    graph.name.return_value = "g"

    from graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints import (
        NodeClassificationPipelineCypherEndpoints,
    )

    endpoints = NodeClassificationPipelineCypherEndpoints(query_runner)
    stream_result = endpoints.predict.stream(graph, model_name="model", include_predicted_probabilities=False)
    mutate_result = endpoints.predict.mutate(
        graph,
        model_name="model",
        mutate_property="predictedClass",
        predicted_probability_property="predictedProbabilities",
    )
    write_result = endpoints.predict.write(
        graph,
        model_name="model",
        write_property="predictedClass",
        predicted_probability_property="predictedProbabilities",
    )

    assert list(stream_result.columns) == ["nodeId", "predictedClass", "predictedProbabilities"]
    assert mutate_result.node_properties_written == 4
    assert write_result.node_properties_written == 8
    assert (
        query_runner.call_procedure.call_args_list[0].kwargs["endpoint"]
        == "gds.beta.pipeline.nodeClassification.predict.stream"
    )
    assert (
        query_runner.call_procedure.call_args_list[1].kwargs["endpoint"]
        == "gds.beta.pipeline.nodeClassification.predict.mutate"
    )
    assert (
        query_runner.call_procedure.call_args_list[2].kwargs["endpoint"]
        == "gds.beta.pipeline.nodeClassification.predict.write"
    )
    assert query_runner.call_procedure.call_args_list[0].kwargs["params"]["config"] == {
        "modelName": "model",
        "includePredictedProbabilities": False,
        "logProgress": True,
        "sudo": False,
        "jobId": query_runner.call_procedure.call_args_list[0].kwargs["params"]["config"]["jobId"],
    }


def test_node_classification_predict_estimate_runs_query() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {
        "nodeCount": 4,
        "relationshipCount": 4,
        "requiredMemory": "42 KiB",
        "treeView": "",
        "mapView": {},
        "bytesMin": 1,
        "bytesMax": 2,
        "heapPercentageMin": 0.1,
        "heapPercentageMax": 0.2,
    }
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))
    graph = mock.Mock()
    graph.name.return_value = "g"

    result = NodeClassificationPipelineCypherEndpoints(query_runner).predict.estimate(
        graph,
        model_name="model",
    )

    assert result.node_count == 4
    assert (
        query_runner.call_procedure.call_args.kwargs["endpoint"]
        == "gds.beta.pipeline.nodeClassification.predict.stream.estimate"
    )


def test_node_classification_model_predict_estimate_delegates_to_predict_endpoints() -> None:
    graph = mock.Mock()
    model_api = mock.Mock()
    predict_endpoints = mock.Mock()
    expected = mock.Mock()
    predict_endpoints.estimate.return_value = expected

    model = NodeClassificationModelV2("model", model_api, predict_endpoints)

    result = model.predict_estimate(graph, concurrency=4)

    assert result is expected
    predict_endpoints.estimate.assert_called_once_with(
        graph,
        model_name="model",
        relationship_types=None,
        target_node_labels=None,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=4,
        job_id=None,
    )


def test_node_classification_add_mlp_runs_query() -> None:
    row = mock.Mock()
    row.to_dict.return_value = _info_payload()
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    result = NodeClassificationPipelineCypherEndpoints(query_runner).add_mlp(
        "pipe",
        batch_size=256,
        class_weights=[0.2, 0.8],
        focus_weight=0.3,
        hidden_layer_sizes=[64, 16, 4],
        learning_rate=0.01,
        max_epochs=12,
        min_epochs=2,
        patience=4,
        penalty=0.1,
        tolerance=0.0002,
    )

    assert result.name == "pipe"
    assert query_runner.call_procedure.call_args.kwargs["endpoint"] == "gds.alpha.pipeline.nodeClassification.addMLP"
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"] == {
        "batchSize": 256,
        "classWeights": [0.2, 0.8],
        "focusWeight": 0.3,
        "hiddenLayerSizes": [64, 16, 4],
        "learningRate": 0.01,
        "maxEpochs": 12,
        "minEpochs": 2,
        "patience": 4,
        "penalty": 0.1,
        "tolerance": 0.0002,
    }


def test_node_classification_add_mlp_uses_default_hidden_layer_sizes() -> None:
    row = mock.Mock()
    row.to_dict.return_value = _info_payload()
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    result = NodeClassificationPipelineCypherEndpoints(query_runner).add_mlp("pipe")

    assert result.name == "pipe"
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"]["hiddenLayerSizes"] == [100]
