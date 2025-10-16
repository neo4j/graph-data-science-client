from __future__ import annotations

from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.procedure_surface.api.config_endpoints import (
    ConfigEndpoints,
    DefaultsEndpoints,
    LimitsEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class ConfigArrowEndpoints(ConfigEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, show_progress: bool = False):
        self._arrow_client = arrow_client
        self._show_progress = show_progress

    @property
    def defaults(self) -> DefaultsEndpoints:
        return DefaultsArrowEndpoints(self._arrow_client, self._show_progress)

    @property
    def limits(self) -> LimitsEndpoints:
        return LimitsArrowEndpoints(self._arrow_client, self._show_progress)


class DefaultsArrowEndpoints(DefaultsEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, show_progress: bool = False):
        self._arrow_client = arrow_client
        self._show_progress = show_progress

    def set(
        self,
        key: str,
        value: Any,
        username: str | None = None,
    ) -> None:
        deserialize(self._arrow_client.do_action_with_retry("v2/defaults.set", {key: value}))

    def list(
        self,
        username: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(
            key=key,
        )

        rows = deserialize(self._arrow_client.do_action_with_retry("v2/defaults.list", config))
        result = {}

        for row in rows:
            result[row["key"]] = row["value"]

        return result


class LimitsArrowEndpoints(LimitsEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, show_progress: bool = False):
        self._arrow_client = arrow_client
        self._show_progress = show_progress

    def set(
        self,
        key: str,
        value: Any,
        username: str | None = None,
    ) -> None:
        deserialize(self._arrow_client.do_action_with_retry("v2/limits.set", {key: value}))

    def list(
        self,
        username: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(
            key=key,
        )

        rows = deserialize(self._arrow_client.do_action_with_retry("v2/limits.list", config))
        result = {}

        for row in rows:
            result[row["key"]] = row["value"]

        return result
