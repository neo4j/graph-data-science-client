from __future__ import annotations

from enum import Enum


class ArrowEndpointVersion(Enum):
    V1 = "v1/"
    V2 = "v2/"

    def version(self) -> str:
        return self._name_.lower()

    def prefix(self) -> str:
        return self._value_

    @staticmethod
    def from_arrow_info(supported_arrow_versions: list[str]) -> ArrowEndpointVersion:
        # If the server supports versioned endpoints, we try v1 first
        if ArrowEndpointVersion.V1.version() in supported_arrow_versions:
            return ArrowEndpointVersion.V1

        raise UnsupportedArrowEndpointVersion(supported_arrow_versions)


class UnsupportedArrowEndpointVersion(Exception):
    def __init__(self, server_version: list[str]) -> None:
        super().__init__(self, f"Unsupported Arrow endpoint versions: {server_version}")
