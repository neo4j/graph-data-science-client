from typing import Any, Optional

from pandas import DataFrame

from ..call_parameters import CallParameters
from ..caller_base import CallerBase
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion


class ConfigProcRunner(IllegalAttrChecker, UncallableNamespace):
    def set(self, key: str, value: Any, username: Optional[str] = None) -> None:
        self._namespace += ".set"

        params = CallParameters(key=key, value=value)

        # Checking for explicit None as '' is a valid user
        if username is not None:
            params["username"] = username

        self._query_runner.call_procedure(endpoint=self._namespace, params=params)

    def list(self, key: Optional[str] = None, username: Optional[str] = None) -> DataFrame:
        self._namespace += ".list"

        config: dict[str, Any] = {}

        if key:
            config["key"] = key
        # Checking for explicit None as '' is a valid user
        if username is not None:
            config["username"] = username

        params = CallParameters(config=config)

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params)


class ConfigIntermediateSteps(CallerBase):
    @property
    def defaults(self) -> ConfigProcRunner:
        return ConfigProcRunner(self._query_runner, f"{self._namespace}.defaults", self._server_version)

    @property
    def limits(self) -> ConfigProcRunner:
        return ConfigProcRunner(self._query_runner, f"{self._namespace}.limits", self._server_version)


class AlphaConfigEndpoints(CallerBase):
    @property
    def config(self) -> ConfigIntermediateSteps:
        return ConfigIntermediateSteps(self._query_runner, f"{self._namespace}.config", self._server_version)


class ConfigEndpoints(CallerBase):
    @property
    @compatible_with("config", min_inclusive=ServerVersion(2, 5, 0))
    def config(self) -> ConfigIntermediateSteps:
        return ConfigIntermediateSteps(self._query_runner, f"{self._namespace}.config", self._server_version)
