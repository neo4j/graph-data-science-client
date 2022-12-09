from typing import Any, Optional

from pandas import DataFrame, Series

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace


class DebugProcRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def sysInfo(self) -> "Series[Any]":
        self._namespace += ".sysInfo"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore


class DirectSystemEndpoints(CallerBase):
    @client_only_endpoint("gds")
    def is_licensed(self) -> bool:
        try:
            license: str = self._query_runner.run_query(
                "CALL gds.debug.sysInfo() YIELD key, value WHERE key = 'gdsEdition' RETURN value"
            ).squeeze()
        except Exception as e:
            # AuraDS does not have `gds.debug.sysInfo`, but is always GDS EE.
            if (
                "There is no procedure with the name `gds.debug.sysInfo` "
                "registered for this database instance." in str(e)
            ):
                license = "Licensed"
            else:
                raise e

        return license == "Licensed"

    @property
    def debug(self) -> DebugProcRunner:
        return DebugProcRunner(self._query_runner, f"{self._namespace}.debug", self._server_version)


class IndirectSystemEndpoints(CallerBase):
    def listProgress(self, job_id: Optional[str] = None) -> DataFrame:
        self._namespace += ".listProgress"

        if job_id:
            query = f"CALL {self._namespace}($job_id)"
            params = {"job_id": job_id}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def userLog(self) -> DataFrame:
        self._namespace += ".userLog"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query)

    def systemMonitor(self) -> "Series[Any]":
        self._namespace += ".systemMonitor"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore

    def backup(self, **config: Any) -> DataFrame:
        self._namespace += ".backup"
        query = f"CALL {self._namespace}($config)"

        return self._query_runner.run_query(query, {"config": config})

    def restore(self, **config: Any) -> DataFrame:
        self._namespace += ".restore"
        query = f"CALL {self._namespace}($config)"

        return self._query_runner.run_query(query, {"config": config})
