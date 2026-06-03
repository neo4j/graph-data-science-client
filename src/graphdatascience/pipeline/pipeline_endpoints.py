import warnings

from ..caller_base import CallerBase
from ..error.client_only_endpoint import deprecated_endpoint_message
from .lp_training_pipeline import LPTrainingPipeline
from .nc_training_pipeline import NCTrainingPipeline
from .nr_training_pipeline import NRTrainingPipeline
from .pipeline_alpha_proc_runner import PipelineAlphaProcRunner, SessionPipelineAlphaProcRunner
from .pipeline_beta_proc_runner import PipelineBetaProcRunner, SessionPipelineBetaProcRunner
from .pipeline_proc_runner import PipelineProcRunner, SessionPipelineProcRunner


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


class SessionPipelineEndpoints(CallerBase):
    @property
    def pipeline(self) -> SessionPipelineProcRunner:
        return SessionPipelineProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)

    def lp_pipe(self, name: str) -> LPTrainingPipeline:
        warnings.warn(
            deprecated_endpoint_message("gds.lp_pipe", "gds.v2.pipeline.link_prediction.create"),
            DeprecationWarning,
        )
        runner = PipelineBetaProcRunner(self._query_runner, f"{self._namespace}.beta.pipeline", self._server_version)
        p, _ = runner.linkPrediction.create(name)
        return p

    def nc_pipe(self, name: str) -> NCTrainingPipeline:
        warnings.warn(
            deprecated_endpoint_message("gds.nc_pipe", "gds.v2.pipeline.node_classification.create"),
            DeprecationWarning,
        )
        runner = PipelineBetaProcRunner(self._query_runner, f"{self._namespace}.beta.pipeline", self._server_version)
        p, _ = runner.nodeClassification.create(name)
        return p

    def nr_pipe(self, name: str) -> NRTrainingPipeline:
        warnings.warn(
            deprecated_endpoint_message("gds.nr_pipe", "gds.v2.pipeline.node_regression.create"),
            DeprecationWarning,
        )
        runner = PipelineAlphaProcRunner(self._query_runner, f"{self._namespace}.alpha.pipeline", self._server_version)
        p, _ = runner.nodeRegression.create(name)
        return p


class SessionPipelineBetaEndpoints(CallerBase):
    @property
    def pipeline(self) -> SessionPipelineBetaProcRunner:
        return SessionPipelineBetaProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)


class SessionPipelineAlphaEndpoints(CallerBase):
    @property
    def pipeline(self) -> SessionPipelineAlphaProcRunner:
        return SessionPipelineAlphaProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)
