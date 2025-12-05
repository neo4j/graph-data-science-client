from ..caller_base import CallerBase
from .lp_training_pipeline import LPTrainingPipeline
from .nc_training_pipeline import NCTrainingPipeline
from .nr_training_pipeline import NRTrainingPipeline
from .pipeline_alpha_proc_runner import PipelineAlphaProcRunner
from .pipeline_beta_proc_runner import PipelineBetaProcRunner
from .pipeline_proc_runner import PipelineProcRunner


class PipelineEndpoints(CallerBase):
    @property
    def pipeline(self) -> PipelineProcRunner:
        return PipelineProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)

    def lp_pipe(self, name: str) -> LPTrainingPipeline:
        """
        Create a Link Prediction training pipeline, with all default settings.

        Args:
            name (str): The name to give the pipeline. Must be unique within the Pipeline Catalog.

        Returns:
            A new instance of a Link Prediction pipeline object.
        """
        runner = PipelineBetaProcRunner(self._query_runner, f"{self._namespace}.beta.pipeline", self._server_version)
        p, _ = runner.linkPrediction.create(name)
        return p

    def nc_pipe(self, name: str) -> NCTrainingPipeline:
        """
        Create a Node Classification training pipeline, with all default settings.

        Args:
            name (str): The name to give the pipeline. Must be unique within the Pipeline Catalog.

        Returns:
            A new instance of a Node Classification pipeline object.
        """

        runner = PipelineBetaProcRunner(self._query_runner, f"{self._namespace}.beta.pipeline", self._server_version)
        p, _ = runner.nodeClassification.create(name)
        return p

    def nr_pipe(self, name: str) -> NRTrainingPipeline:
        """
        Create a Node Regression training pipeline, with all default settings.

        Args:
            name (str): The name to give the pipeline. Must be unique within the Pipeline Catalog.

        Returns:
            A new instance of a Node Regression pipeline object.
        """
        runner = PipelineAlphaProcRunner(self._query_runner, f"{self._namespace}.alpha.pipeline", self._server_version)
        p, _ = runner.nodeRegression.create(name)
        return p


class PipelineBetaEndpoints(CallerBase):
    @property
    def pipeline(self) -> PipelineBetaProcRunner:
        return PipelineBetaProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)


class PipelineAlphaEndpoints(CallerBase):
    @property
    def pipeline(self) -> PipelineAlphaProcRunner:
        return PipelineAlphaProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)
