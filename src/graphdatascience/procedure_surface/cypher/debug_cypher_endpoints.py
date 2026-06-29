from __future__ import annotations

from graphdatascience.procedure_surface.api.debug_endpoints import DebugEndpoints, DebugSysInfoResult
from graphdatascience.query_runner.query_runner import QueryRunner


class DebugCypherEndpoints(DebugEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def sys_info(self) -> DebugSysInfoResult:
        result = self._query_runner.call_procedure(endpoint="gds.debug.sysInfo")
        # The procedure yields one (key, value) row per entry; pivot into a single object.
        key_values = dict(zip(result["key"], result["value"]))
        return DebugSysInfoResult(**key_values)
