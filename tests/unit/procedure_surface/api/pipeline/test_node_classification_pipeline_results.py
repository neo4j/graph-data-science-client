from graphdatascience.procedure_surface.api.pipeline import (
    NodeClassificationPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationModelInfoResult,
)


def test_node_classification_train_result_materializes_typed_model_info() -> None:
    result = NodeClassificationPipelineTrainResult(
        trainMillis=7,
        modelInfo={
            "modelName": "model",
            "modelType": "NodeClassification",
            "bestParameters": {"maxDepth": 3},
            "metrics": {"F1_WEIGHTED": 0.9},
            "pipeline": {"nodePropertySteps": []},
        },
    )

    assert isinstance(result.model_info, NodeClassificationModelInfoResult)
    assert result.model_info is not None
    assert result.model_info.model_name == "model"
    assert result.model_info.model_type == "NodeClassification"
    assert result.model_info.best_parameters == {"maxDepth": 3}
