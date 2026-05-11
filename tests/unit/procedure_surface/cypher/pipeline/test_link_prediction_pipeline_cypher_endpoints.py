from typing import Any, cast
from unittest import mock

import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModelV2
from graphdatascience.procedure_surface.api.pipeline import PipelineCatalogEntry
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.cypher.pipeline.link_prediction_pipeline_cypher_endpoints import (
    LinkPredictionPipelineCypherEndpoints,
)


def test_link_prediction_create_returns_info_result() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    pipeline, result = LinkPredictionPipelineCypherEndpoints(query_runner).create("pipe")

    assert pipeline.name() == "pipe"
    assert result.name == "pipe"
    assert query_runner.call_procedure.call_args.kwargs["endpoint"] == "gds.beta.pipeline.linkPrediction.create"


def test_link_prediction_add_feature_runs_query() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    result = LinkPredictionPipelineCypherEndpoints(query_runner).add_feature(
        "pipe",
        "HADAMARD",
        top_k=10,
    )

    assert result.name == "pipe"
    assert query_runner.call_procedure.call_args.kwargs["endpoint"] == "gds.beta.pipeline.linkPrediction.addFeature"
    assert query_runner.call_procedure.call_args.kwargs["params"]["feature_type"] == "HADAMARD"
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"] == {"topK": 10}


def test_link_prediction_add_node_property_runs_query_with_config() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    result = LinkPredictionPipelineCypherEndpoints(query_runner).add_node_property(
        "pipe",
        "pageRank",
        mutate_property="pr",
        max_iterations=10,
    )

    assert result.name == "pipe"
    assert (
        query_runner.call_procedure.call_args.kwargs["endpoint"] == "gds.beta.pipeline.linkPrediction.addNodeProperty"
    )
    assert query_runner.call_procedure.call_args.kwargs["params"]["procedure_name"] == "pageRank"
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"] == {
        "mutateProperty": "pr",
        "maxIterations": 10,
    }


def test_link_prediction_configure_auto_tuning_runs_query() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "autoTuningConfig": {"maxTrials": 42}}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    result = LinkPredictionPipelineCypherEndpoints(query_runner).configure_auto_tuning("pipe", max_trials=42)

    assert result.name == "pipe"
    assert (
        query_runner.call_procedure.call_args.kwargs["endpoint"]
        == "gds.alpha.pipeline.linkPrediction.configureAutoTuning"
    )
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"] == {"maxTrials": 42}


def test_link_prediction_get_uses_shared_pipeline_catalog() -> None:
    query_runner = mock.Mock()
    pipeline_catalog = mock.Mock(spec=PipelineCatalogProtocol)
    pipeline_catalog.exists.return_value = PipelineCatalogEntry(
        pipelineName="pipe", pipelineType="Link prediction training pipeline"
    )

    with mock.patch(
        "graphdatascience.procedure_surface.cypher.pipeline.link_prediction_pipeline_cypher_endpoints.PipelineCatalogCypherEndpoints",
        return_value=pipeline_catalog,
    ) as pipeline_catalog_cls:
        pipeline = LinkPredictionPipelineCypherEndpoints(query_runner).get("pipe")

    assert pipeline.name() == "pipe"
    pipeline_catalog_cls.assert_called_once_with(query_runner)
    pipeline_catalog.exists.assert_called_once_with("pipe")
    query_runner.call_procedure.assert_not_called()


def test_link_prediction_endpoints_do_not_accept_pipeline_catalog_constructor_override() -> None:
    query_runner = mock.Mock()
    constructor = mock.Mock(side_effect=LinkPredictionPipelineCypherEndpoints)

    with mock.patch(
        "graphdatascience.procedure_surface.cypher.pipeline.link_prediction_pipeline_cypher_endpoints.PipelineCatalogCypherEndpoints"
    ):
        with pytest.raises(TypeError, match="pipeline_catalog"):
            constructor(query_runner, pipeline_catalog=mock.Mock())


def test_link_prediction_train_runs_query() -> None:
    create_row = mock.Mock()
    create_row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    row = mock.Mock()
    row.to_dict.return_value = {"trainMillis": 7, "modelInfo": {"modelName": "model"}, "modelSelectionStats": {}}
    query_runner = mock.Mock()
    query_runner.call_procedure.side_effect = [
        mock.Mock(squeeze=mock.Mock(return_value=create_row)),
        mock.Mock(squeeze=mock.Mock(return_value=row)),
    ]
    graph = mock.Mock()
    graph.name.return_value = "g"

    pipeline, _ = LinkPredictionPipelineCypherEndpoints(query_runner).create("pipe")
    model, result = pipeline.train(graph, model_name="model")

    assert isinstance(model, LinkPredictionModelV2)
    assert model.name() == "model"
    assert result.train_millis == 7
    assert result.model_info is not None
    assert result.model_info.model_name == "model"
    assert query_runner.call_procedure.call_args_list[1].kwargs["endpoint"] == "gds.beta.pipeline.linkPrediction.train"


def test_link_prediction_train_estimate_runs_query() -> None:
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

    result = LinkPredictionPipelineCypherEndpoints(query_runner).train.estimate(
        graph,
        "pipe",
        model_name="model",
    )

    assert result.node_count == 4
    assert query_runner.call_procedure.call_args.kwargs["endpoint"] == "gds.beta.pipeline.linkPrediction.train.estimate"


def test_link_prediction_predict_stream_and_mutate_run_queries() -> None:
    mutate_row = mock.Mock()
    mutate_row.to_dict.return_value = {
        "computeMillis": 1,
        "mutateMillis": 3,
        "relationshipsWritten": 4,
        "postProcessingMillis": 5,
        "preProcessingMillis": 2,
        "configuration": {},
    }
    query_runner = mock.Mock()
    query_runner.call_procedure.side_effect = [
        pd.DataFrame({"sourceNodeId": [0], "targetNodeId": [1], "probability": [0.9]}),
        mock.Mock(squeeze=mock.Mock(return_value=mutate_row)),
    ]
    graph = mock.Mock()
    graph.name.return_value = "g"

    endpoints = LinkPredictionPipelineCypherEndpoints(query_runner)
    stream_result = endpoints.predict.stream(graph, model_name="model", top_n=2)
    mutate_result = endpoints.predict.mutate(
        graph,
        model_name="model",
        mutate_relationship_type="PREDICTED",
        top_n=2,
    )

    assert list(stream_result.columns) == ["sourceNodeId", "targetNodeId", "probability"]
    assert mutate_result.relationships_written == 4
    assert (
        query_runner.call_procedure.call_args_list[0].kwargs["endpoint"]
        == "gds.beta.pipeline.linkPrediction.predict.stream"
    )
    assert (
        query_runner.call_procedure.call_args_list[1].kwargs["endpoint"]
        == "gds.beta.pipeline.linkPrediction.predict.mutate"
    )
    assert query_runner.call_procedure.call_args_list[0].kwargs["params"]["config"]["topN"] == 2
    assert query_runner.call_procedure.call_args_list[1].kwargs["params"]["config"]["topN"] == 2


def test_link_prediction_predict_estimate_runs_query() -> None:
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

    result = LinkPredictionPipelineCypherEndpoints(query_runner).predict.estimate(
        graph,
        model_name="model",
        top_n=2,
    )

    assert result.node_count == 4
    assert (
        query_runner.call_procedure.call_args.kwargs["endpoint"]
        == "gds.beta.pipeline.linkPrediction.predict.stream.estimate"
    )
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"]["topN"] == 2
