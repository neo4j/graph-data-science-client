from .model import Model


class LPModel(Model):
    def _query_prefix(self) -> str:
        return "CALL gds.beta.pipeline.linkPrediction.predict."
