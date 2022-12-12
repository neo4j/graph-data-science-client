from ..caller_base import CallerBase
from .lp_training_pipeline import LPTrainingPipeline
from .nc_training_pipeline import NCTrainingPipeline
from .nr_training_pipeline import NRTrainingPipeline
from .pipeline_proc_runner import PipelineProcRunner


class PipelineEndpoints(CallerBase):
    @property
    def pipeline(self) -> PipelineProcRunner:
        return PipelineProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)

    def lp_pipe(self, name: str) -> LPTrainingPipeline:
        p, _ = self.beta.pipeline.linkPrediction.create(name)
        return p

    def nc_pipe(self, name: str) -> NCTrainingPipeline:
        p, _ = self.beta.pipeline.nodeClassification.create(name)
        return p

    def nr_pipe(self, name: str) -> NRTrainingPipeline:
        p, _ = self.alpha.pipeline.nodeRegression.create(name)
        return p
