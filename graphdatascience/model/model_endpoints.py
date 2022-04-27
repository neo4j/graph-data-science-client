from ..caller_base import CallerBase
from .model_proc_runner import ModelProcRunner


class ModelEndpoints(CallerBase):
    @property
    def model(self) -> ModelProcRunner:
        return ModelProcRunner(self._query_runner, f"{self._namespace}.model", self._server_version)
