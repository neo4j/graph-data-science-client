from ..query_runner.query_runner import QueryRunner
from .pipeline_proc_runner import PipelineProcRunner


class PipelineEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def pipeline(self) -> PipelineProcRunner:
        return PipelineProcRunner(self._query_runner, f"{self._namespace}.pipeline")
