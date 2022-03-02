from .model import Model


class LPModel(Model):
    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.linkPrediction.predict."
