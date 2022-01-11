from gdsclient.model.trained_model import TrainedModel

from ..query_runner.query_runner import QueryRunner


class LPPredictionPipeline(TrainedModel):
    def __init__(self, name: str, query_runner: QueryRunner) -> None:
        super().__init__(name, query_runner)

    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.linkPrediction.predict."
