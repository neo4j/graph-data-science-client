from ..caller_base import CallerBase
from .model_alpha_proc_runner import ModelAlphaProcRunner
from .model_beta_proc_runner import ModelBetaProcRunner
from .model_proc_runner import ModelProcRunner


class ModelEndpoints(CallerBase):
    @property
    def model(self) -> ModelProcRunner:
        return ModelProcRunner(self._query_runner, f"{self._namespace}.model", self._server_version)


class ModelBetaEndpoints(CallerBase):
    @property
    def model(self) -> ModelBetaProcRunner:
        return ModelBetaProcRunner(self._query_runner, f"{self._namespace}.model", self._server_version)


class ModelAlphaEndpoints(CallerBase):
    @property
    def model(self) -> ModelAlphaProcRunner:
        return ModelAlphaProcRunner(self._query_runner, f"{self._namespace}.model", self._server_version)
