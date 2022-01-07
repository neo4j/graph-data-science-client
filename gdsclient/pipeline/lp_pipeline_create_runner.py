from ..query_runner.query_runner import QueryRunner
from .lp_training_pipeline import LPTrainingPipeline


class LPPipelineCreateRunner:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def create(self, name: str) -> LPTrainingPipeline:
        self._namespace += ".create"

        query = f"CALL {self._namespace}($name)"
        params = {"name": name}
        self._query_runner.run_query(query, params)

        return LPTrainingPipeline(name, self._query_runner)
