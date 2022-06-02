from .pipeline_model import PipelineModel


class NRModel(PipelineModel):
    def _query_prefix(self) -> str:
        return "CALL gds.alpha.pipeline.nodeRegression.predict."
