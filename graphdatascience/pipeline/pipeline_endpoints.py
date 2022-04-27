from ..caller_base import CallerBase
from .pipeline_proc_runner import PipelineProcRunner


class PipelineEndpoints(CallerBase):
    @property
    def pipeline(self) -> PipelineProcRunner:
        return PipelineProcRunner(self._query_runner, f"{self._namespace}.pipeline", self._server_version)
