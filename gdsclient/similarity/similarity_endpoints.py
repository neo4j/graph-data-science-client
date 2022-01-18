from typing import List

from ..query_runner.query_runner import QueryRunner


class SimilarityEndpoints:
    # NOTE: We cannot use "similarity" as an endpoint in this class
    # since that will clash with e.g. `gds.alpha.similarity.cosine.write`
    # which should trigger "write" of the AlgoEndpoints.

    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def jaccard(self, vector1: List[int], vector2: List[int]) -> float:
        namespace = self._namespace + ".jaccard"

        result = self._query_runner.run_query(
            f"RETURN {namespace}({vector1}, {vector2}) AS similarity"
        )

        return result[0]["similarity"]  # type: ignore
