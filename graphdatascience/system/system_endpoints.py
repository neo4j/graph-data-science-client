from typing import Optional

from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace


class DebugProcRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def sysInfo(self) -> Series:
        self._namespace += ".sysInfo"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore


class SystemEndpoints(CallerBase):
    def listProgress(self, job_id: Optional[str] = None) -> DataFrame:
        self._namespace += ".listProgress"

        if job_id:
            query = f"CALL {self._namespace}($job_id)"
            params = {"job_id": job_id}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def systemMonitor(self) -> Series:
        self._namespace += ".systemMonitor"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore

    @property
    def debug(self) -> DebugProcRunner:
        return DebugProcRunner(self._query_runner, f"{self._namespace}.debug", self._server_version)

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
