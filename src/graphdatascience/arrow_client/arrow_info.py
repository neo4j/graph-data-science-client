from __future__ import annotations

from dataclasses import dataclass

from neo4j.exceptions import Neo4jError

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
        try:
            procResult = query_runner.call_procedure(
                endpoint="gds.debug.arrow",
                query_type=QueryType.SYSTEM,
                custom_error=False,
                yields=["listenAddress", "enabled", "running", "versions"],
            ).iloc[0]
        except Neo4jError as e:
            if ArrowInfo._procedure_not_registered(e):
                return ArrowInfo(listenAddress="", enabled=False, running=False, versions=[])
            raise

        return ArrowInfo(
            listenAddress=procResult["listenAddress"],
            enabled=procResult["enabled"],
            running=procResult["running"],
            versions=procResult.get("versions", []),
        )

    @staticmethod
    def _procedure_not_registered(e: Neo4jError) -> bool:
        return e.code == "Neo.ClientError.Procedure.ProcedureNotFound" or (
            "no procedure with the name `gds.debug.arrow`" in e.message
        )
