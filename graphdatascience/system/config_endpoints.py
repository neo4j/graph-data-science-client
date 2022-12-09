from typing import Any, Dict, Optional

from pandas import DataFrame

from graphdatascience.caller_base import CallerBase
from graphdatascience.error.illegal_attr_checker import IllegalAttrChecker
from graphdatascience.error.uncallable_namespace import UncallableNamespace


class ConfigProcRunner(CallerBase, IllegalAttrChecker, UncallableNamespace):
    def set(self, key: str, value: Any, username: Optional[str] = None) -> None:
        self._namespace += ".set"

        params = {"key": key, "value": value}

        # Checking for explicit None as '' is a valid user
        if username is not None:
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
        if username is not None:
            config["username"] = username

        query = f"CALL {self._namespace}($config)"
        params = {"config": config}

        return self._query_runner.run_query(query, params)


class IndirectConfigEndpoints(CallerBase):
    @property
    def defaults(self) -> ConfigProcRunner:
        return ConfigProcRunner(self._query_runner, f"{self._namespace}.defaults", self._server_version)

    @property
    def limits(self) -> ConfigProcRunner:
        return ConfigProcRunner(self._query_runner, f"{self._namespace}.limits", self._server_version)
