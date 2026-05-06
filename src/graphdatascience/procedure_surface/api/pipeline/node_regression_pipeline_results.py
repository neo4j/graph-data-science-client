from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult


class NodeRegressionPipelineInfoResult(BaseResult):
    auto_tuning_config: dict[str, Any] | None = None
    feature_properties: list[Any] | None = None
    name: str | None = None
    node_property_steps: list[Any] | None = None
    parameter_space: dict[str, Any] | None = None
    split_config: dict[str, Any] | None = None


class NodeRegressionPipelineCreateResult(NodeRegressionPipelineInfoResult):
    pass


class NodeRegressionPipelineTrainResult(BaseResult):
    configuration: dict[str, Any] | None = None
    model_info: dict[str, Any] | None = None
    model_selection_stats: dict[str, Any] | None = None
    train_millis: int | None = None
