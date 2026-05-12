from graphdatascience.procedure_surface.api.pipeline import (
    NodeClassificationPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationModelInfoResult,
)


def _train_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "configuration": {},
        "modelInfo": {
            "bestParameters": {"maxDepth": 3},
            "metrics": {"F1_WEIGHTED": 0.9},
            "modelName": "model",
            "modelType": "NodeClassification",
            "pipeline": {"nodePropertySteps": []},
        },
        "modelSelectionStats": {},
        "trainMillis": 7,
    }
    payload.update(overrides)
    return payload


def test_node_classification_train_result_materializes_typed_model_info() -> None:
    result = NodeClassificationPipelineTrainResult(**_train_payload())

    assert isinstance(result.model_info, NodeClassificationModelInfoResult)
    assert result.model_info.model_name == "model"
    assert result.model_info.model_type == "NodeClassification"
    assert result.model_info.best_parameters == {"maxDepth": 3}
