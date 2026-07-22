from gds_cli.app import app
from typer.testing import CliRunner

runner = CliRunner()


def test_root_help_lists_command_groups() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "database" in result.output
    assert "session" in result.output


def test_database_help_lists_commands() -> None:
    result = runner.invoke(app, ["database", "--help"])

    assert result.exit_code == 0
    for command in ("upload", "summary", "fetch", "delete"):
        assert command in result.output


def test_session_help_lists_commands() -> None:
    result = runner.invoke(app, ["session", "--help"])

    assert result.exit_code == 0
    for command in ("create", "delete", "list", "run", "project", "algorithms", "writeback", "drop"):
        assert command in result.output
