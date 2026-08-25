from __future__ import annotations

from enum import Enum

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient


class ArrowEndpointVersion(Enum):
    V1 = "v1"
    V2 = "v2"

    def version(self) -> str:
        return self._name_.lower()

    def prefix(self) -> str:
        return f"{self._value_}/"

    @staticmethod
    def check_version_compatibility(
        compatible_versions: set[ArrowEndpointVersion], arrow_client: AuthenticatedArrowClient
    ) -> None:
        supported_server_versions = {
            action.type.split("/")[0].lower() for action in arrow_client.list_actions_with_retry()
        }
        for version in compatible_versions:
            if version.version() in supported_server_versions:
                return

        raise UnsupportedArrowEndpointVersion(compatible_versions, supported_server_versions)


class UnsupportedArrowEndpointVersion(Exception):
    def __init__(self, compatible_versions: set[ArrowEndpointVersion], supported_server_versions: set[str]) -> None:
        super().__init__(
            f"The GDS version is not supported by this client version, please update the `graphdatascience` package. Arrow versions supported by this client are: {compatible_versions}, but GDS supports: {supported_server_versions}",
        )
