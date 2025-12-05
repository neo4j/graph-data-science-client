from __future__ import annotations

from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.config_endpoints import (
    ConfigEndpoints,
    DefaultsEndpoints,
    LimitsEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class ConfigCypherEndpoints(ConfigEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    @property
    def defaults(self) -> DefaultsEndpoints:
        return DefaultsCypherEndpoints(self._query_runner)

    @property
    def limits(self) -> LimitsEndpoints:
        return LimitsCypherEndpoints(self._query_runner)


class DefaultsCypherEndpoints(DefaultsEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def set(
        self,
        key: str,
        value: Any,
        username: str | None = None,
    ) -> None:
        key = ConfigConverter.convert_to_camel_case(key)

        params = {
            "key": key,
            "value": value,
        }

        if username:
            params["username"] = username

        params = CallParameters(**params)

        self._query_runner.call_procedure(endpoint="gds.config.defaults.set", params=params)

    def list(
        self,
        username: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(
            key=key,
            username=username,
        )

        params = CallParameters(
            config=config,
        )

        result = self._query_runner.call_procedure(endpoint="gds.config.defaults.list", params=params)
        return {row["key"]: row["value"] for _, row in result.iterrows()}


class LimitsCypherEndpoints(LimitsEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def set(
        self,
        key: str,
        value: Any,
        username: str | None = None,
    ) -> None:
        key = ConfigConverter.convert_to_camel_case(key)

        params = {
            "key": key,
            "value": value,
        }

        if username:
            params["username"] = username

        params = CallParameters(**params)

        self._query_runner.call_procedure(endpoint="gds.config.limits.set", params=params)

    def list(
        self,
        username: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(
            key=key,
            username=username,
        )

        params = CallParameters(
            config=config,
        )

        result = self._query_runner.call_procedure(endpoint="gds.config.limits.list", params=params)
        return {row["key"]: row["value"] for _, row in result.iterrows()}
