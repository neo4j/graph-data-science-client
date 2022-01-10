from .algo.algo_endpoints import AlgoEndpoints
from .graph.graph_endpoints import GraphEndpoints
from .pipeline.pipeline_endpoints import PipelineEndpoints
from .model.model_endpoints import ModelEndpoints
from .query_runner.query_runner import QueryRunner


class IndirectEndpoints(
    AlgoEndpoints, GraphEndpoints, ModelEndpoints, PipelineEndpoints
):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        super().__init__(query_runner, namespace)
