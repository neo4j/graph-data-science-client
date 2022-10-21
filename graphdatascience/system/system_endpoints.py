from typing import Any, Dict, Optional

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

    def set(self, key: str, value: Any, username: Optional[str] = None) -> None:
        self._namespace += ".set"

        params = {"key": key, "value": value}

        # Checking for explicit None as '' is a valid user
        if username != None:
            query = f"CALL {self._namespace}($key, $value, $username)"
            params["username"] = username
        else:
            query = f"CALL {self._namespace}($key, $value)"

        self._query_runner.run_query(query, params)

    def list(self, key: Optional[str] = None, username: Optional[str] = None) -> DataFrame:
        self._namespace += ".list"

        config: Dict[str, Any] = {}

        if key:
            config["key"] = key
        # Checking for explicit None as '' is a valid user
        if username != None:
            config["username"] = username

        query = f"CALL {self._namespace}($config)"
        params = {"config": config}

        return self._query_runner.run_query(query, params)
