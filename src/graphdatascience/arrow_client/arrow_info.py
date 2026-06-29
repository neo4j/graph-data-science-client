from __future__ import annotations

from dataclasses import dataclass

from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


@dataclass(frozen=True)
class ArrowInfo:
    listenAddress: str
    enabled: bool
    running: bool
    versions: list[str]

    @staticmethod
    def create(query_runner: QueryRunner) -> ArrowInfo:
        procResult = query_runner.call_procedure(
            endpoint="gds.debug.arrow",
            query_type=QueryType.SYSTEM,
            custom_error=False,
            yields=["listenAddress", "enabled", "running", "versions"],
        ).iloc[0]

        return ArrowInfo(
            listenAddress=procResult["listenAddress"],
            enabled=procResult["enabled"],
            running=procResult["running"],
            versions=procResult.get("versions", []),
        )
