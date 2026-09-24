from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult


class GraphSageRuntimeTrainResult(BaseResult):
    configuration: dict[str, Any]
    pre_processing_millis: int
    train_millis: int


class GraphSageRuntimeMutateResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    pre_processing_millis: int


class GraphSageRuntimeWriteResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    node_properties_written: int
    pre_processing_millis: int
    write_millis: int
