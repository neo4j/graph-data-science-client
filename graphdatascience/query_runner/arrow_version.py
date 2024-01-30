from enum import Enum


class ArrowVersion(Enum):
    ALPHA = ""
    V1 = "v1/"

    def name(self):
        return self._name_.lower()

    def prefix(self):
        return self._value_


class UnsupportedArrowVersion(Exception):
    def __init__(self, server_version):
        super().__init__(self, f"Unsupported Arrow server version: {server_version}")
