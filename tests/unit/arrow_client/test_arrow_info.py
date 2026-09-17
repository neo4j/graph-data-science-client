import pytest
from neo4j.exceptions import AuthError, Neo4jError
from pandas import DataFrame

from graphdatascience.arrow_client.arrow_info import ArrowInfo
from tests.unit.conftest import CollectingQueryRunner


def procedure_not_found() -> Neo4jError:
    # hydration mirrors how the driver constructs server-side errors, including the error code
    return Neo4jError._hydrate_neo4j(
        code="Neo.ClientError.Procedure.ProcedureNotFound",
        message="There is no procedure with the name `gds.debug.arrow` registered for this database instance.",
    )


def test_create_returns_arrow_info(runner: CollectingQueryRunner) -> None:
    runner.add__mock_result(
        "gds.debug.arrow",
        DataFrame([{"listenAddress": "localhost:8491", "enabled": True, "running": True, "versions": ["1.0.0"]}]),
    )

    assert ArrowInfo.create(runner) == ArrowInfo(
        listenAddress="localhost:8491", enabled=True, running=True, versions=["1.0.0"]
    )


def test_create_reports_disabled_if_procedure_not_registered(runner: CollectingQueryRunner) -> None:
    runner.add__mock_result("gds.debug.arrow", procedure_not_found())

    assert ArrowInfo.create(runner) == ArrowInfo(listenAddress="", enabled=False, running=False, versions=[])


def test_create_reports_disabled_if_error_code_unknown(runner: CollectingQueryRunner) -> None:
    error = Neo4jError._hydrate_neo4j(
        code="Neo.DatabaseError.General.UnknownError",
        message="There is no procedure with the name `gds.debug.arrow` registered for this database instance.",
    )
    runner.add__mock_result("gds.debug.arrow", error)

    assert ArrowInfo.create(runner) == ArrowInfo(listenAddress="", enabled=False, running=False, versions=[])


def test_create_propagates_unrelated_client_errors(runner: CollectingQueryRunner) -> None:
    error = Neo4jError._hydrate_neo4j(
        code="Neo.ClientError.Security.Unauthorized", message="Unsupported authentication token, scheme `none`."
    )
    runner.add__mock_result("gds.debug.arrow", error)

    with pytest.raises(AuthError):
        ArrowInfo.create(runner)
