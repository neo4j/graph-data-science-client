from ..query_runner.query_runner import QueryRunner
from .lp_pipeline import LPPipeline


class LPPipelineCreateRunner:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def create(self, name: str) -> LPPipeline:
        self._namespace += ".create"

        query = f"CALL {self._namespace}($name)"
        params = {"name": name}
        self._query_runner.run_query(query, params)

        return LPPipeline(name, self._query_runner)
