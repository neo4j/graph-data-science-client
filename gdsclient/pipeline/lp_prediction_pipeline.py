from gdsclient.model.trained_model import TrainedModel


class LPPredictionPipeline(TrainedModel):
    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.linkPrediction.predict."
