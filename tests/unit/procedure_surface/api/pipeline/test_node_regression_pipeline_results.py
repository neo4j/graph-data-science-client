import pytest
from pydantic import ValidationError

from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_results import (
    NodeRegressionModelInfoResult,
    NodeRegressionPipelineTrainResult,
)


def _train_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "configuration": {},
        "modelInfo": {
            "bestParameters": {"maxDepth": 3},
            "metrics": {"MAE": 0.1},
            "modelName": "model",
            "modelType": "NodeRegression",
            "pipeline": {"nodePropertySteps": []},
        },
        "modelSelectionStats": {},
        "trainMillis": 7,
    }
    payload.update(overrides)
    return payload


def test_node_regression_train_result_materializes_typed_model_info() -> None:
    result = NodeRegressionPipelineTrainResult(**_train_payload())

    assert isinstance(result.model_info, NodeRegressionModelInfoResult)
    assert result.model_info.model_name == "model"
    assert result.model_info.model_type == "NodeRegression"
    assert result.model_info.best_parameters == {"maxDepth": 3}
