from .pipeline_model import PipelineModel


class LPModel(PipelineModel):
    def _query_prefix(self) -> str:
        return "CALL gds.beta.pipeline.linkPrediction.predict."
