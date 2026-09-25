from typing import Any

from pydantic import Field

from graphdatascience.procedure_surface.api.base_result import BaseResult


class GraphSageSupervisedTrainResult(BaseResult):
    configuration: dict[str, Any]
    pre_processing_millis: int
    # the arrow summary reports the training time as `train_ms`
    train_millis: int = Field(alias="train_ms")


class GraphSageUnsupervisedTrainResult(BaseResult):
    configuration: dict[str, Any]
    pre_processing_millis: int
    # the arrow summary reports the training time as `train_ms`
    train_millis: int = Field(alias="train_ms")


class GraphSageSupervisedMutateResult(BaseResult):
    compute_millis: int = Field(alias="predict_ms")
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    pre_processing_millis: int


class GraphSageUnsupervisedMutateResult(BaseResult):
    compute_millis: int = Field(alias="predict_ms")
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    pre_processing_millis: int


class GraphSageSupervisedWriteResult(BaseResult):
    compute_millis: int = Field(alias="predict_ms")
    configuration: dict[str, Any]
    node_properties_written: int
    pre_processing_millis: int
    write_millis: int


class GraphSageUnsupervisedWriteResult(BaseResult):
    compute_millis: int = Field(alias="predict_ms")
    configuration: dict[str, Any]
    node_properties_written: int
    pre_processing_millis: int
    write_millis: int
