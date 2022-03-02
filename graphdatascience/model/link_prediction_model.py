from .trained_model import TrainedModel


class LPModel(TrainedModel):
    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.linkPrediction.predict."
