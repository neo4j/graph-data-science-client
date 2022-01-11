from .algo.algo_endpoints import AlgoEndpoints
from .graph.graph_endpoints import GraphEndpoints
from .model.model_endpoints import ModelEndpoints
from .pipeline.pipeline_endpoints import PipelineEndpoints
from .query_runner.query_runner import QueryRunner
from .system.system_endpoints import SystemEndpoints


class IndirectEndpoints(
    AlgoEndpoints, GraphEndpoints, ModelEndpoints, PipelineEndpoints, SystemEndpoints
):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        super().__init__(query_runner, namespace)
