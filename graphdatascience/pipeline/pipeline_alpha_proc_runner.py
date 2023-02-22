from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .nr_pipeline_create_runner import NRPipelineCreateRunner


class PipelineAlphaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def nodeRegression(self) -> NRPipelineCreateRunner:
        return NRPipelineCreateRunner(self._query_runner, f"{self._namespace}.nodeRegression", self._server_version)
