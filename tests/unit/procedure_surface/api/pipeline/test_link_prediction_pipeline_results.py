from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionModelInfoResult,
    LinkPredictionPipelineInfoResult,
    LinkPredictionPipelineTrainResult,
)


def test_link_prediction_pipeline_info_result_uses_snake_case_fields() -> None:
    result = LinkPredictionPipelineInfoResult(
        name="pipe",
        splitConfig={"trainFraction": 0.7},
        featureSteps=[{"name": "L2", "config": {"nodeProperties": ["embedding"]}}],
    )

    assert result.name == "pipe"
    assert result.split_config == {"trainFraction": 0.7}
    assert result.feature_steps == [{"name": "L2", "config": {"nodeProperties": ["embedding"]}}]


def test_link_prediction_train_result_materializes_typed_model_info() -> None:
    result = LinkPredictionPipelineTrainResult(
        trainMillis=7,
        modelInfo={
            "modelName": "model",
            "modelType": "LinkPrediction",
            "bestParameters": {"maxDepth": 3},
            "metrics": {"AUCPR": 0.1},
            "pipeline": {"featureSteps": []},
        },
    )

    assert isinstance(result.model_info, LinkPredictionModelInfoResult)
    assert result.model_info is not None
    assert result.model_info.model_name == "model"
    assert result.model_info.model_type == "LinkPrediction"
    assert result.model_info.best_parameters == {"maxDepth": 3}


def test_link_prediction_train_result_allows_missing_model_info() -> None:
    result = LinkPredictionPipelineTrainResult(trainMillis=7)

    assert result.model_info is None
