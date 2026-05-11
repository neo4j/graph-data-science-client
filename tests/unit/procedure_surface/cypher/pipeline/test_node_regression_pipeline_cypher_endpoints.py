from typing import Any, cast
from unittest import mock

import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.pipeline import PipelineCatalogEntry
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.cypher.pipeline import NodeRegressionPipelineCypherEndpoints


def test_node_regression_add_linear_regression_runs_query() -> None:
    create_row = mock.Mock()
    create_row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    step_row = mock.Mock()
    step_row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.side_effect = [
        mock.Mock(squeeze=mock.Mock(return_value=create_row)),
        mock.Mock(squeeze=mock.Mock(return_value=step_row)),
    ]

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    pipeline, _ = NodeRegressionPipelineCypherEndpoints(query_runner).create("pipe")
    result = pipeline.add_linear_regression(penalty=1.0)

    assert result.name == "pipe"
    assert pipeline.name() == "pipe"
    assert (
        query_runner.call_procedure.call_args_list[0].kwargs["endpoint"] == "gds.alpha.pipeline.nodeRegression.create"
    )
    assert (
        query_runner.call_procedure.call_args_list[1].kwargs["endpoint"]
        == "gds.alpha.pipeline.nodeRegression.addLinearRegression"
    )


def test_node_regression_add_linear_regression_accepts_tuple_range_inputs() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    NodeRegressionPipelineCypherEndpoints(query_runner).add_linear_regression(
        "pipe",
        max_epochs=(5, 20),
        penalty=(0.0, 0.5),
    )

    assert query_runner.call_procedure.call_args.kwargs["params"]["config"] == {
        "batchSize": 100,
        "learningRate": 0.001,
        "maxEpochs": {"range": [5, 20]},
        "minEpochs": 1,
        "patience": 1,
        "penalty": {"range": [0.0, 0.5]},
        "tolerance": 0.001,
    }


def test_node_regression_add_linear_regression_rejects_list_range_inputs() -> None:
    query_runner = mock.Mock()

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    with pytest.raises(ValueError, match="max_epochs range inputs must be tuples with exactly two values."):
        NodeRegressionPipelineCypherEndpoints(query_runner).add_linear_regression(
            "pipe",
            max_epochs=cast(Any, [5, 20]),
        )


def test_node_regression_add_node_property_runs_query_with_config() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"name": "pipe", "featureProperties": []}
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = mock.Mock(squeeze=mock.Mock(return_value=row))

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    result = NodeRegressionPipelineCypherEndpoints(query_runner).add_node_property(
        "pipe",
        "pageRank",
        mutate_property="pr",
        max_iterations=10,
    )

    assert result.name == "pipe"
    assert query_runner.call_procedure.call_args.kwargs["params"]["procedure_name"] == "pageRank"
    assert query_runner.call_procedure.call_args.kwargs["params"]["config"] == {
        "mutateProperty": "pr",
        "maxIterations": 10,
    }


def test_node_regression_get_runs_query() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame(
        [{"pipelineName": "pipe", "pipelineType": "Node regression training pipeline"}]
    )

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    pipeline = NodeRegressionPipelineCypherEndpoints(query_runner).get("pipe")

    assert pipeline.name() == "pipe"
    query_runner.call_procedure.assert_called_once_with(
        "gds.pipeline.list",
        params=mock.ANY,
        custom_error=False,
    )
    assert query_runner.call_procedure.call_args.kwargs["params"] == {"pipeline_name": "pipe"}


def test_node_regression_train_runs_query() -> None:
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

    from graphdatascience.procedure_surface.api.model.node_regression_model import NodeRegressionModelV2
    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    pipeline, _ = NodeRegressionPipelineCypherEndpoints(query_runner).create("pipe")
    model, result = pipeline.train(graph, metrics=["MAE"], model_name="model", target_property="y")

    assert isinstance(model, NodeRegressionModelV2)
    assert model.name() == "model"
    assert result.train_millis == 7
    assert result.model_info is not None
    assert result.model_info.model_name == "model"
    assert query_runner.call_procedure.call_args_list[1].kwargs["endpoint"] == "gds.alpha.pipeline.nodeRegression.train"


def test_node_regression_train_accepts_pipeline_name() -> None:
    row = mock.Mock()
    row.to_dict.return_value = {"trainMillis": 7, "modelInfo": {"modelName": "model"}, "modelSelectionStats": {}}
    query_runner = mock.Mock()
    query_runner.call_procedure.side_effect = [
        mock.Mock(squeeze=mock.Mock(return_value=row)),
        mock.Mock(squeeze=mock.Mock(return_value=row)),
    ]
    graph = mock.Mock()
    graph.name.return_value = "g"

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    endpoints = NodeRegressionPipelineCypherEndpoints(query_runner)
    endpoints.train(graph, "pipe", metrics=["MAE"], model_name="model", target_property="y")


def test_node_regression_get_uses_shared_pipeline_catalog() -> None:
    query_runner = mock.Mock()
    pipeline_catalog = mock.Mock(spec=PipelineCatalogProtocol)
    pipeline_catalog.exists.return_value = PipelineCatalogEntry(
        pipelineName="pipe", pipelineType="Node regression training pipeline"
    )

    with mock.patch(
        "graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints.PipelineCatalogCypherEndpoints",
        return_value=pipeline_catalog,
    ) as pipeline_catalog_cls:
        pipeline = NodeRegressionPipelineCypherEndpoints(query_runner).get("pipe")

    assert pipeline.name() == "pipe"
    pipeline_catalog_cls.assert_called_once_with(query_runner)
    pipeline_catalog.exists.assert_called_once_with("pipe")
    query_runner.call_procedure.assert_not_called()


def test_node_regression_endpoints_do_not_accept_pipeline_catalog_constructor_override() -> None:
    query_runner = mock.Mock()

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    constructor = mock.Mock(side_effect=NodeRegressionPipelineCypherEndpoints)

    with mock.patch(
        "graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints.PipelineCatalogCypherEndpoints"
    ):
        with pytest.raises(TypeError, match="pipeline_catalog"):
            constructor(query_runner, pipeline_catalog=mock.Mock())


def test_node_regression_predict_stream_and_mutate_run_queries() -> None:
    mutate_row = mock.Mock()
    mutate_row.to_dict.return_value = {
        "computeMillis": 1,
        "mutateMillis": 3,
        "nodePropertiesWritten": 4,
        "postProcessingMillis": 5,
        "preProcessingMillis": 2,
        "configuration": {},
    }
    query_runner = mock.Mock()
    query_runner.call_procedure.side_effect = [
        pd.DataFrame({"nodeId": [0], "predictedValue": [1.0]}),
        mock.Mock(squeeze=mock.Mock(return_value=mutate_row)),
    ]
    graph = mock.Mock()
    graph.name.return_value = "g"

    from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
        NodeRegressionPipelineCypherEndpoints,
    )

    endpoints = NodeRegressionPipelineCypherEndpoints(query_runner)
    stream_result = endpoints.predict.stream(graph, model_name="model")
    mutate_result = endpoints.predict.mutate(graph, model_name="model", mutate_property="predicted")

    assert list(stream_result.columns) == ["nodeId", "predictedValue"]
    assert mutate_result.node_properties_written == 4
    assert mutate_result.compute_millis == 1
    assert (
        query_runner.call_procedure.call_args_list[0].kwargs["endpoint"]
        == "gds.alpha.pipeline.nodeRegression.predict.stream"
    )
    assert (
        query_runner.call_procedure.call_args_list[1].kwargs["endpoint"]
        == "gds.alpha.pipeline.nodeRegression.predict.mutate"
    )
