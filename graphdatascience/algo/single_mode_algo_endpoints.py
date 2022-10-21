from ..caller_base import CallerBase
from .algo_proc_runner import StreamModeRunner


# these algorithms have only one mode
class SingleModeAlgoEndpoints(CallerBase):
    @property
    def triangles(self) -> StreamModeRunner:
        return StreamModeRunner(self._query_runner, f"{self._namespace}.triangles", self._server_version)
