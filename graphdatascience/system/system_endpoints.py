from typing import Optional

from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..query_runner.query_runner import QueryRunner


class DebugProcRunner(UncallableNamespace, IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def sysInfo(self) -> Series:
        self._namespace += ".sysInfo"
        query = f"CALL {self._namespace}()"

        return self._query_runner.run_query(query).squeeze()  # type: ignore


class SystemEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

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
        return DebugProcRunner(self._query_runner, f"{self._namespace}.debug")
