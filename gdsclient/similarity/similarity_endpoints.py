from ..query_runner.query_runner import QueryRunner
from .similarity_algo_runner import SimilarityAlgoRunner


class SimilarityEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def jaccard(self) -> SimilarityAlgoRunner:
        return SimilarityAlgoRunner(self._query_runner, f"{self._namespace}.jaccard")

    @property
    def cosine(self) -> SimilarityAlgoRunner:
        return SimilarityAlgoRunner(self._query_runner, f"{self._namespace}.cosine")

    @property
    def pearson(self) -> SimilarityAlgoRunner:
        return SimilarityAlgoRunner(self._query_runner, f"{self._namespace}.pearson")

    @property
    def euclideanDistance(self) -> SimilarityAlgoRunner:
        return SimilarityAlgoRunner(
            self._query_runner, f"{self._namespace}.euclideanDistance"
        )

    @property
    def euclidean(self) -> SimilarityAlgoRunner:
        return SimilarityAlgoRunner(self._query_runner, f"{self._namespace}.euclidean")

    @property
    def overlap(self) -> SimilarityAlgoRunner:
        return SimilarityAlgoRunner(self._query_runner, f"{self._namespace}.overlap")

    @property
    def ann(self) -> SimilarityAlgoRunner:
        return SimilarityAlgoRunner(self._query_runner, f"{self._namespace}.ann")
