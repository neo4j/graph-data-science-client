import re
from typing import Any, Optional

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion


class DebugProcRunner(UncallableNamespace, IllegalAttrChecker):
    def sysInfo(self) -> "Series[Any]":
        self._namespace += ".sysInfo"
        return self._query_runner.call_procedure(endpoint=self._namespace).squeeze()  # type: ignore

    def arrow(self) -> "Series[Any]":
        self._namespace += ".arrow"
        return self._query_runner.call_procedure(endpoint=self._namespace).squeeze()  # type: ignore


class MemoryProcRunner(UncallableNamespace, IllegalAttrChecker):
    @compatible_with("memory.list", min_inclusive=ServerVersion(2, 13, 0))
    def list(self) -> "Series[Any]":
        return self._query_runner.call_procedure(endpoint=self._namespace).squeeze()  # type: ignore

    @compatible_with("memory.summary", min_inclusive=ServerVersion(2, 13, 0))
    def summary(self) -> "Series[Any]":
        self._namespace += ".summary"
        return self._query_runner.call_procedure(endpoint=self._namespace).squeeze()  # type: ignore


class LicenseProcRunner(UncallableNamespace, IllegalAttrChecker):
    def state(self) -> "Series[Any]":
        self._namespace += ".state"

        try:
            return self._query_runner.call_procedure(endpoint=self._namespace).squeeze()  # type: ignore
        except Exception as e:
            # AuraDS does not have `gds.license.state`, but is always GDS EE.
            if re.match(r"There is no procedure with the name `gds.*` registered for this database instance", str(e)):
                return Series({"edition": "Licensed", "details": "AuraDS"})
            else:
                raise e


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
            isLicensed: bool = self._query_runner.run_cypher(query, custom_error=False).squeeze()
        except Exception as e:
            # AuraDS does not have `gds.isLicensed`, but is always GDS EE.
            if re.match(r".*Unknown function 'gds.isLicensed'.*", str(e)):
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

    @compatible_with("backup", min_inclusive=ServerVersion(2, 5, 0))
    def backup(self, **config: Any) -> DataFrame:
        namespace = self._namespace + ".backup"
        return self._query_runner.call_procedure(endpoint=namespace, params=CallParameters(config=config))

    @compatible_with("restore", min_inclusive=ServerVersion(2, 5, 0))
    def restore(self, **config: Any) -> DataFrame:
        namespace = self._namespace + ".restore"
        return self._query_runner.call_procedure(endpoint=namespace, params=CallParameters(config=config))

    @compatible_with("listProgress", min_inclusive=ServerVersion(2, 5, 0))
    def listProgress(self, job_id: Optional[str] = None) -> DataFrame:
        return SystemBetaEndpoints(self._query_runner, self._namespace, self._server_version).listProgress(job_id)

    @property
    def memory(self) -> MemoryProcRunner:
        return MemoryProcRunner(self._query_runner, f"{self._namespace}.debug", self._server_version)

    @compatible_with("systemMonitor", min_inclusive=ServerVersion(2, 5, 0))
    def systemMonitor(self) -> "Series[Any]":
        return SystemAlphaEndpoints(self._query_runner, self._namespace, self._server_version).systemMonitor()

    @compatible_with("userLog", min_inclusive=ServerVersion(2, 5, 0))
    def userLog(self) -> DataFrame:
        return SystemAlphaEndpoints(self._query_runner, self._namespace, self._server_version).userLog()


class SystemBetaEndpoints(CallerBase):
    def listProgress(self, job_id: Optional[str] = None) -> DataFrame:
        self._namespace += ".listProgress"

        params = CallParameters()
        if job_id:
            params["job_id"] = job_id

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params)


class SystemAlphaEndpoints(CallerBase):
    def userLog(self) -> DataFrame:
        self._namespace += ".userLog"
        return self._query_runner.call_procedure(endpoint=self._namespace)

    def systemMonitor(self) -> "Series[Any]":
        self._namespace += ".systemMonitor"
        return self._query_runner.call_procedure(endpoint=self._namespace).squeeze()  # type: ignore

    def backup(self, **config: Any) -> DataFrame:
        self._namespace += ".backup"
        return self._query_runner.call_procedure(endpoint=self._namespace, params=CallParameters(config=config))

    def restore(self, **config: Any) -> DataFrame:
        self._namespace += ".restore"
        return self._query_runner.call_procedure(endpoint=self._namespace, params=CallParameters(config=config))
