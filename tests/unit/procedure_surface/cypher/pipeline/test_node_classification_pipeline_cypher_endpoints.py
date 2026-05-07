from unittest import mock

import pandas as pd

from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineInfoResult,
)
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints import (
    NodeClassificationPipelineCypherEndpoints,
)


def test_node_classification_train_runs_query() -> None:
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


def test_node_classification_create_returns_info_result() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    pipeline, result = NodeClassificationPipelineCypherEndpoints(query_runner).create("pipe")

    assert pipeline.name() == "pipe"
    assert type(result) is NodeClassificationPipelineInfoResult
    assert result.name == "pipe"


def test_node_classification_get_uses_pipeline_list() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame(
        [{"pipelineName": "pipe", "pipelineType": "Node classification training pipeline"}]
    )

    pipeline = NodeClassificationPipelineCypherEndpoints(query_runner).get("pipe")

    assert pipeline.name() == "pipe"
    query_runner.call_procedure.assert_called_once_with(
        "gds.pipeline.list",
        params=mock.ANY,
        custom_error=False,
    )
    assert query_runner.call_procedure.call_args.kwargs["params"] == {"pipeline_name": "pipe"}


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
    stream_result = endpoints.predict.stream(graph, model_name="model")
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


def test_node_classification_add_mlp_runs_query() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    result = NodeClassificationPipelineCypherEndpoints(query_runner).add_mlp(
        "pipe", hidden_layer_sizes=[64, 16, 4], penalty=0.1
    )

    assert result.name == "pipe"
    assert query_runner.call_procedure.call_args.kwargs["endpoint"] == "gds.alpha.pipeline.nodeClassification.addMLP"
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"] == {
        "hiddenLayerSizes": [64, 16, 4],
        "penalty": 0.1,
    }
