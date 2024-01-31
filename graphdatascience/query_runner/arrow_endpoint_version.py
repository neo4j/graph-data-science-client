from __future__ import annotations

from enum import Enum
from typing import List


class ArrowEndpointVersion(Enum):
    ALPHA = ""
    V1 = "v1/"

    def version(self) -> str:
        return self._name_.lower()

    def prefix(self) -> str:
        return self._value_

    @staticmethod
    def from_arrow_info(supported_arrow_versions: List[str]) -> ArrowEndpointVersion:
        # Fallback for pre 2.6.0 servers that do not support versions
        if len(supported_arrow_versions) == 0:
            return ArrowEndpointVersion.ALPHA

        # If the server supports versioned endpoints, we try v1 first
        if ArrowEndpointVersion.V1.version() in supported_arrow_versions:
            return ArrowEndpointVersion.V1

        if ArrowEndpointVersion.ALPHA.version() in supported_arrow_versions:
            return ArrowEndpointVersion.ALPHA

        raise UnsupportedArrowEndpointVersion(supported_arrow_versions)


class UnsupportedArrowEndpointVersion(Exception):
    def __init__(self, server_version: List[str]) -> None:
        super().__init__(self, f"Unsupported Arrow endpoint versions: {server_version}")
