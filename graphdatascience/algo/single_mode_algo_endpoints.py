from ..caller_base import CallerBase
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .algo_proc_runner import StreamModeRunner


# these algorithms have only one mode
class SingleModeAlgoEndpoints(CallerBase):
    @property
    @compatible_with("triangles", min_inclusive=ServerVersion(2, 5, 0))
    def triangles(self) -> StreamModeRunner:
        # todo insert job id
        return StreamModeRunner(self._query_runner, f"{self._namespace}.triangles", self._server_version)


class SingleModeAlphaAlgoEndpoints(CallerBase):
    @property
    def triangles(self) -> StreamModeRunner:
        return StreamModeRunner(self._query_runner, f"{self._namespace}.triangles", self._server_version)
