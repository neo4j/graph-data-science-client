from typing import Any, Optional

from pandas import DataFrame, Series

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from graphdatascience.server_version.server_version import ServerVersion


class DebugProcRunner(UncallableNamespace, IllegalAttrChecker):
    def sysInfo(self) -> "Series[Any]":
        self._namespace += ".sysInfo"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore

    def arrow(self) -> "Series[Any]":
        self._namespace += ".arrow"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore


class LicenseProcRunner(UncallableNamespace, IllegalAttrChecker):
    def state(self) -> "Series[Any]":
        self._namespace += ".state"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore


class DirectSystemEndpoints(CallerBase):
    @client_only_endpoint("gds")
    def is_licensed(self) -> bool:
        if self._server_version >= ServerVersion(2, 5, 0):
            query = "RETURN gds.isLicensed()"
        else:
            query = """
            CALL gds.debug.sysInfo()
            YIELD key, value
            WHERE key = 'gdsEdition'
            RETURN
                CASE value
                    WHEN 'Licensed' THEN true
                    ELSE false
                END
            """

        try:
            isLicensed: bool = self._query_runner.run_query(query, custom_error=False).squeeze()
        except Exception as e:
            # AuraDS does not have `gds.debug.sysInfo`, but is always GDS EE.
            if (
                "There is no procedure with the name `gds.debug.sysInfo` "
                "registered for this database instance." in str(e)
            ):
                isLicensed = True
            else:
                raise e

        return isLicensed

    @property
    def license(self) -> LicenseProcRunner:
        return LicenseProcRunner(self._query_runner, f"{self._namespace}.license", self._server_version)

    @property
    def debug(self) -> DebugProcRunner:
        return DebugProcRunner(self._query_runner, f"{self._namespace}.debug", self._server_version)
    
    def backup(self, **config: Any) -> DataFrame:
        self._namespace += ".backup"
        query = f"CALL {self._namespace}($config)"

        return self._query_runner.run_query(query, {"config": config})


class SystemBetaEndpoints(CallerBase):
    def listProgress(self, job_id: Optional[str] = None) -> DataFrame:
        self._namespace += ".listProgress"

        if job_id:
            query = f"CALL {self._namespace}($job_id)"
            params = {"job_id": job_id}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)


class SystemAlphaEndpoints(CallerBase):
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
