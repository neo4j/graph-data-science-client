from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_results import (
    NodeRegressionModelInfoResult,
    NodeRegressionPipelineTrainResult,
)


def test_node_regression_train_result_materializes_typed_model_info() -> None:
    result = NodeRegressionPipelineTrainResult(
        trainMillis=7,
        modelInfo={
            "modelName": "model",
            "modelType": "NodeRegression",
            "bestParameters": {"maxDepth": 3},
            "metrics": {"MAE": 0.1},
            "pipeline": {"nodePropertySteps": []},
        },
    )

    assert isinstance(result.model_info, NodeRegressionModelInfoResult)
    assert result.model_info is not None
    assert result.model_info.model_name == "model"
    assert result.model_info.model_type == "NodeRegression"
    assert result.model_info.best_parameters == {"maxDepth": 3}


def test_node_regression_train_result_allows_missing_model_info() -> None:
    result = NodeRegressionPipelineTrainResult(trainMillis=7)

    assert result.model_info is None
