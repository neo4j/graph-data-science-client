from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult


class NodeRegressionPipelineInfoResult(BaseResult):
    auto_tuning_config: dict[str, Any]
    feature_properties: list[Any]
    name: str
    node_property_steps: list[Any]
    parameter_space: dict[str, Any]
    split_config: dict[str, Any]


class NodeRegressionModelInfoResult(BaseResult):
    best_parameters: dict[str, Any]
    metrics: dict[str, Any]
    model_name: str
    model_type: str
    pipeline: dict[str, Any]


class NodeRegressionPipelineTrainResult(BaseResult):
    configuration: dict[str, Any]
    model_info: NodeRegressionModelInfoResult
    model_selection_stats: dict[str, Any]
    train_millis: int
