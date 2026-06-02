from __future__ import annotations

import warnings
from typing import Any

from pandas import DataFrame, Series

from ..error.client_only_endpoint import deprecated_endpoint_message
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..model.pipeline_model import PipelineModel
from .lp_pipeline_create_runner import LPPipelineCreateRunner
from .nc_pipeline_create_runner import NCPipelineCreateRunner
from .pipeline_proc_runner import PipelineProcRunner
from .training_pipeline import TrainingPipeline


class PipelineBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def linkPrediction(self) -> LPPipelineCreateRunner:
        return LPPipelineCreateRunner(self._query_runner, f"{self._namespace}.linkPrediction", self._server_version)

    @property
    def nodeClassification(self) -> NCPipelineCreateRunner:
        return NCPipelineCreateRunner(self._query_runner, f"{self._namespace}.nodeClassification", self._server_version)

    def list(self, pipeline: TrainingPipeline[PipelineModel] | None = None) -> DataFrame:
        return PipelineProcRunner(self._query_runner, self._namespace, self._server_version).list(pipeline)

    def exists(self, pipeline_name: str) -> Series[Any]:
        return PipelineProcRunner(self._query_runner, self._namespace, self._server_version).exists(pipeline_name)

    def drop(self, pipeline: TrainingPipeline[PipelineModel]) -> Series[Any]:
        return PipelineProcRunner(self._query_runner, self._namespace, self._server_version).drop(pipeline)


class SessionPipelineBetaProcRunner(PipelineBetaProcRunner):
    @property
    def linkPrediction(self) -> LPPipelineCreateRunner:
        warnings.warn(
            deprecated_endpoint_message(
                "gds.beta.pipeline.linkPrediction.create",
                "gds.v2.pipeline.link_prediction.create",
            ),
            DeprecationWarning,
        )
        return super().linkPrediction

    @property
    def nodeClassification(self) -> NCPipelineCreateRunner:
        warnings.warn(
            deprecated_endpoint_message(
                "gds.beta.pipeline.nodeClassification.create",
                "gds.v2.pipeline.node_classification.create",
            ),
            DeprecationWarning,
        )
        return super().nodeClassification

    def list(self, pipeline: TrainingPipeline[PipelineModel] | None = None) -> DataFrame:
        warnings.warn(
            deprecated_endpoint_message("gds.beta.pipeline.list", "gds.v2.pipeline.list"),
            DeprecationWarning,
        )
        return super().list(pipeline)

    def exists(self, pipeline_name: str) -> Series[Any]:
        warnings.warn(
            deprecated_endpoint_message("gds.beta.pipeline.exists", "gds.v2.pipeline.exists"),
            DeprecationWarning,
        )
        return super().exists(pipeline_name)

    def drop(self, pipeline: TrainingPipeline[PipelineModel]) -> Series[Any]:
        warnings.warn(
            deprecated_endpoint_message("gds.beta.pipeline.drop", "gds.v2.pipeline.drop"),
            DeprecationWarning,
        )
        return super().drop(pipeline)
