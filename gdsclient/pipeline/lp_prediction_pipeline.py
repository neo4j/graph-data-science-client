from ..query_runner.query_runner import QueryRunner
from .prediction_pipeline import PredictionPipeline


class LPPredictionPipeline(PredictionPipeline):
    def __init__(self, name: str, query_runner: QueryRunner) -> None:
        super().__init__(name, query_runner)

    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.linkPrediction.predict."
