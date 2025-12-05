from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult


class NodeSimilarityMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    post_processing_millis: int
    nodes_compared: int
    relationships_written: int
    similarity_distribution: dict[str, Any]
    configuration: dict[str, Any]


class NodeSimilarityStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    nodes_compared: int
    similarity_pairs: int
    similarity_distribution: dict[str, Any]
    configuration: dict[str, Any]


class NodeSimilarityWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    post_processing_millis: int
    nodes_compared: int
    relationships_written: int
    similarity_distribution: dict[str, Any]
    configuration: dict[str, Any]
