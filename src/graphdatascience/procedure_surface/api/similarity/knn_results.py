from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult


class KnnMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    post_processing_millis: int
    nodes_compared: int
    relationships_written: int
    similarity_distribution: dict[str, int | float]
    did_converge: bool
    ran_iterations: int
    node_pairs_considered: int
    configuration: dict[str, Any]


class KnnStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    nodes_compared: int
    similarity_pairs: int
    similarity_distribution: dict[str, int | float]
    did_converge: bool
    ran_iterations: int
    node_pairs_considered: int
    configuration: dict[str, Any]


class KnnWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    post_processing_millis: int
    nodes_compared: int
    relationships_written: int
    did_converge: bool
    ran_iterations: int
    node_pairs_considered: int
    similarity_distribution: dict[str, int | float]
    configuration: dict[str, Any]
