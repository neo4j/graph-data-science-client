from gds_cli.app import app
from typer.testing import CliRunner

runner = CliRunner()


def test_root_help_lists_command_groups() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in ("run", "database", "sessions"):
        assert command in result.output


def test_database_help_lists_commands() -> None:
    result = runner.invoke(app, ["database", "--help"])

    assert result.exit_code == 0
    for command in ("upload", "summary", "fetch", "delete"):
        assert command in result.output


def test_db_is_an_alias_for_database() -> None:
    result = runner.invoke(app, ["db", "--help"])

    assert result.exit_code == 0
    for command in ("upload", "summary", "fetch", "delete"):
        assert command in result.output


def test_db_alias_is_not_a_separate_help_entry() -> None:
    # the alias resolves at dispatch but is not listed as its own command group
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    # `database` is the only entry in the Commands column; `db` appears only in its help text
    assert "│ db " not in result.output


def test_sessions_help_lists_commands() -> None:
    result = runner.invoke(app, ["sessions", "--help"])

    assert result.exit_code == 0
    for command in ("create", "delete", "list"):
        assert command in result.output


def test_run_help_shows_options() -> None:
    result = runner.invoke(app, ["run", "--help"])

    assert result.exit_code == 0
    assert "--overwrite-graph" in result.output
    assert "--session-name" in result.output
