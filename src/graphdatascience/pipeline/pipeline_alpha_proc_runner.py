import warnings

from ..error.client_only_endpoint import deprecated_endpoint_message
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .nr_pipeline_create_runner import NRPipelineCreateRunner


class PipelineAlphaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def nodeRegression(self) -> NRPipelineCreateRunner:
        return NRPipelineCreateRunner(self._query_runner, f"{self._namespace}.nodeRegression", self._server_version)


class SessionPipelineAlphaProcRunner(PipelineAlphaProcRunner):
    @property
    def nodeRegression(self) -> NRPipelineCreateRunner:
        warnings.warn(
            deprecated_endpoint_message(
                "gds.alpha.pipeline.nodeRegression.create",
                "gds.v2.pipeline.node_regression.create",
            ),
            DeprecationWarning,
        )
        return super().nodeRegression
