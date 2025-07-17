from __future__ import annotations

from dataclasses import dataclass

from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion


@dataclass(frozen=True)
class ArrowInfo:
    listenAddress: str
    enabled: bool
    running: bool
    versions: list[str]

    @staticmethod
    def create(query_runner: QueryRunner) -> ArrowInfo:
        debugYields = ["listenAddress", "enabled", "running"]
        if query_runner.server_version() > ServerVersion(2, 6, 0):
            debugYields.append("versions")

        procResult = query_runner.call_procedure(
            endpoint="gds.debug.arrow", custom_error=False, yields=debugYields
        ).iloc[0]

        return ArrowInfo(
            listenAddress=procResult["listenAddress"],
            enabled=procResult["enabled"],
            running=procResult["running"],
            versions=procResult.get("versions", []),
        )
