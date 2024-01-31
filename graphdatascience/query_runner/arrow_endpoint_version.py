from __future__ import annotations

from enum import Enum


class ArrowEndpointVersion(Enum):
    ALPHA = ""
    V1 = "v1/"

    def name(self) -> str:
        return self._name_.lower()

    def prefix(self) -> str:
        return self._value_

    @staticmethod
    def from_arrow_info(arrow_info: Series[Any]) -> ArrowEndpointVersion:
        supported_arrow_versions = arrow_info.get("versions", [])
        # Fallback for pre 2.6.0 servers that do not support versions
        if len(supported_arrow_versions) == 0:
            return ArrowEndpointVersion.ALPHA

        # If the server supports versioned endpoints, we try v1 first
        if ArrowEndpointVersion.V1.name() in supported_arrow_versions:
            return ArrowEndpointVersion.V1

        if ArrowEndpointVersion.ALPHA.name() in supported_arrow_versions:
            return ArrowEndpointVersion.ALPHA

        raise UnsupportedArrowEndpointVersion(supported_arrow_versions)


class UnsupportedArrowEndpointVersion(Exception):
    def __init__(self, server_version):
        super().__init__(self, f"Unsupported Arrow endpoint versions: {server_version}")
