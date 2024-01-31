from enum import Enum


class ArrowEndpointVersion(Enum):
    ALPHA = ""
    V1 = "v1/"

    def name(self):
        return self._name_.lower()

    def prefix(self):
        return self._value_


class UnsupportedArrowEndpointVersion(Exception):
    def __init__(self, server_version):
        super().__init__(self, f"Unsupported Arrow endpoint version: {server_version}")
