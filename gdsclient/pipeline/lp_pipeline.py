from ..query_runner.query_runner import QueryRunner


class LPPipeline:
    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    def name(self) -> str:
        return self._name
