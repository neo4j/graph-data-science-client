from .model import Model


class NRModel(Model):
    def _query_prefix(self) -> str:
        return "CALL gds.alpha.pipeline.nodeRegression.predict."
