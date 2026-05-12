import pytest
from pydantic import ValidationError

from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionModelInfoResult,
    LinkPredictionPipelineInfoResult,
    LinkPredictionPipelineTrainResult,
)


def _info_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "autoTuningConfig": {},
        "featureSteps": [],
        "name": "pipe",
        "nodePropertySteps": [],
        "parameterSpace": {},
        "splitConfig": {"trainFraction": 0.7},
    }
    payload.update(overrides)
    return payload


def _train_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "configuration": {},
        "modelInfo": {
            "bestParameters": {"maxDepth": 3},
            "metrics": {"AUCPR": 0.1},
            "modelName": "model",
            "modelType": "LinkPrediction",
            "pipeline": {"featureSteps": []},
        },
        "modelSelectionStats": {},
        "trainMillis": 7,
    }
    payload.update(overrides)
    return payload


def test_link_prediction_pipeline_info_result_uses_snake_case_fields() -> None:
    result = LinkPredictionPipelineInfoResult(
        **_info_payload(featureSteps=[{"name": "L2", "config": {"nodeProperties": ["embedding"]}}])
    )

    assert result.name == "pipe"
    assert result.split_config == {"trainFraction": 0.7}
    assert result.feature_steps == [{"name": "L2", "config": {"nodeProperties": ["embedding"]}}]


def test_link_prediction_train_result_materializes_typed_model_info() -> None:
    result = LinkPredictionPipelineTrainResult(**_train_payload())

    assert isinstance(result.model_info, LinkPredictionModelInfoResult)
    assert result.model_info.model_name == "model"
    assert result.model_info.model_type == "LinkPrediction"
    assert result.model_info.best_parameters == {"maxDepth": 3}
