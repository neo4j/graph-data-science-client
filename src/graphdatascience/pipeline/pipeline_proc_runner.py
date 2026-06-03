from __future__ import annotations

import warnings
from typing import Any

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..error.client_only_endpoint import client_only_endpoint, deprecated_endpoint_message
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..model.pipeline_model import PipelineModel
from ..server_version.server_version import ServerVersion
from .lp_training_pipeline import LPTrainingPipeline
from .nc_training_pipeline import NCTrainingPipeline
from .nr_training_pipeline import NRTrainingPipeline
from .training_pipeline import TrainingPipeline


class PipelineProcRunner(UncallableNamespace, IllegalAttrChecker):
    @client_only_endpoint("gds.pipeline")
    def get(self, pipeline_name: str) -> TrainingPipeline[PipelineModel]:
        # as it was a client only endpoint, exists need
        if self._server_version < ServerVersion(2, 5, 0):
            self._namespace = "gds.beta.pipeline"
        result = self.exists(pipeline_name)
        if result["exists"]:
            return self._resolve_pipeline(result["pipelineType"], result["pipelineName"])
        else:
            raise ValueError(f"No pipeline named '{pipeline_name}' exists")

    def list(self, pipeline: TrainingPipeline[PipelineModel] | None = None) -> DataFrame:
        self._namespace += ".list"

        params = CallParameters()
        if pipeline:
            params["pipeline_name"] = pipeline.name()

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params)

    def exists(self, pipeline_name: str) -> Series[Any]:
        self._namespace += ".exists"

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace, params=CallParameters(pipeline_name=pipeline_name)
        ).squeeze()

    def drop(self, pipeline: TrainingPipeline[PipelineModel]) -> Series[Any]:
        self._namespace += ".drop"

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace, params=CallParameters(pipeline_name=pipeline.name())
        ).squeeze()

    def _resolve_pipeline(self, pipeline_type: str, pipeline_name: str) -> TrainingPipeline[PipelineModel]:
        if pipeline_type == "Node classification training pipeline":
            return NCTrainingPipeline(pipeline_name, self._query_runner, self._server_version)
        elif pipeline_type == "Link prediction training pipeline":
            return LPTrainingPipeline(pipeline_name, self._query_runner, self._server_version)
        elif pipeline_type == "Node regression training pipeline":
            return NRTrainingPipeline(pipeline_name, self._query_runner, self._server_version)

        raise ValueError(f"Unknown model type encountered: '{pipeline_type}'")


class SessionPipelineProcRunner(PipelineProcRunner):
    def get(self, pipeline_name: str) -> TrainingPipeline[PipelineModel]:
        warnings.warn(
            deprecated_endpoint_message("gds.pipeline.get", "gds.v2.pipeline.<pipeline_type>.get"),
            DeprecationWarning,
        )
        result = PipelineProcRunner.exists(self, pipeline_name)
        if result["exists"]:
            return self._resolve_pipeline(result["pipelineType"], result["pipelineName"])

        raise ValueError(f"No pipeline named '{pipeline_name}' exists")

    def list(self, pipeline: TrainingPipeline[PipelineModel] | None = None) -> DataFrame:
        warnings.warn(deprecated_endpoint_message("gds.pipeline.list", "gds.v2.pipeline.list"), DeprecationWarning)
        return super().list(pipeline)

    def exists(self, pipeline_name: str) -> Series[Any]:
        warnings.warn(deprecated_endpoint_message("gds.pipeline.exists", "gds.v2.pipeline.exists"), DeprecationWarning)
        return super().exists(pipeline_name)

    def drop(self, pipeline: TrainingPipeline[PipelineModel]) -> Series[Any]:
        warnings.warn(deprecated_endpoint_message("gds.pipeline.drop", "gds.v2.pipeline.drop"), DeprecationWarning)
        return super().drop(pipeline)
