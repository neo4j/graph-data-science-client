from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from gds_cli.session.commands import _connect
from gds_cli.session.commands import app as sessions_app
from gds_cli.session.config import ComputeSpec, JobsConfig, JobSpec, ProjectSpec, SessionConfig
from gds_cli.session.report import JobReport
from typer.testing import CliRunner

runner = CliRunner()

_CONFIG_WITH_NAME = (
    "session:\n"
    "  name: from-file\n"
    "  memory: 2GB\n"
    "  ttl: 30m\n"
    "jobs:\n"
    "  - project:\n"
    "      type: cypher\n"
    "      query: RETURN gds.graph.project.remote(n, n)\n"
    "    compute:\n"
    "      - compute: louvain\n"
    "        config:\n"
    "          resultProperty: community\n"
)


def _cfg(n_jobs: int = 1) -> JobsConfig:
    return JobsConfig(
        session=SessionConfig(memory="2GB", ttl="30m"),
        jobs=[
            JobSpec(
                project=ProjectSpec(type="cypher", query="MATCH (n) RETURN gds.graph.project.remote(n, n)"),
                compute=[ComputeSpec(compute="louvain", config={"resultProperty": "community"})],
            )
            for _ in range(n_jobs)
        ],
    )


def _session_info(status: str, session_id: str) -> MagicMock:
    info = MagicMock()
    info.status = status
    info.id = session_id
    return info


def test_connect_deletes_and_recreates_a_deleted_session() -> None:
    cfg = _cfg()
    fake_gds = MagicMock()

    with (
        patch("gds_cli.session.session_ops.find_session") as fake_find,
        patch("gds_cli.session.session_ops.delete") as fake_delete,
        patch("gds_cli.session.session_ops.connect", return_value=fake_gds) as fake_connect,
    ):
        fake_find.side_effect = [_session_info("Deleting", "old-id"), _session_info("Creating", "new-id")]
        gds = _connect(cfg.session, "my-session", JobReport())

    fake_delete.assert_called_once_with("my-session")
    fake_connect.assert_called_once()
    assert gds is fake_gds


def test_connect_does_not_delete_a_healthy_existing_session() -> None:
    cfg = _cfg()
    fake_gds = MagicMock()

    with (
        patch("gds_cli.session.session_ops.find_session", return_value=_session_info("Ready", "id-1")),
        patch("gds_cli.session.session_ops.delete") as fake_delete,
        patch("gds_cli.session.session_ops.connect", return_value=fake_gds),
    ):
        _connect(cfg.session, "my-session", JobReport())

    fake_delete.assert_not_called()


def test_connect_recreates_even_if_cleanup_delete_fails() -> None:
    cfg = _cfg()
    fake_gds = MagicMock()

    with (
        patch("gds_cli.session.session_ops.find_session") as fake_find,
        patch("gds_cli.session.session_ops.delete", side_effect=RuntimeError("boom")),
        patch("gds_cli.session.session_ops.connect", return_value=fake_gds) as fake_connect,
    ):
        fake_find.side_effect = [_session_info("Deleted", "old-id"), None]
        gds = _connect(cfg.session, "my-session", JobReport())

    fake_connect.assert_called_once()
    assert gds is fake_gds


# --- gds sessions create / list / delete ---


def test_sessions_create_from_cli_args() -> None:
    with (
        patch("gds_cli.session.session_ops.find_session", return_value=None),
        patch("gds_cli.session.session_ops.connect") as connect,
    ):
        result = runner.invoke(sessions_app, ["create", "--name", "s1", "--memory", "4GB", "--ttl", "45"])

    assert result.exit_code == 0, result.output
    connect.assert_called_once()
    session, name = connect.call_args.args[0], connect.call_args.args[1]
    assert name == "s1"
    assert (session.memory, session.ttl) == ("4GB", timedelta(minutes=45))
    assert "Session 's1' ready." in result.output


def test_sessions_create_from_file(tmp_path: Path) -> None:
    cfg = tmp_path / "job.yaml"
    cfg.write_text(_CONFIG_WITH_NAME)

    with (
        patch("gds_cli.session.session_ops.find_session", return_value=None),
        patch("gds_cli.session.session_ops.connect") as connect,
    ):
        result = runner.invoke(sessions_app, ["create", "-f", str(cfg)])

    assert result.exit_code == 0, result.output
    session, name = connect.call_args.args[0], connect.call_args.args[1]
    assert name == "from-file"  # read from session.name
    assert session.memory == "2GB"


def test_sessions_create_cli_overrides_file(tmp_path: Path) -> None:
    cfg = tmp_path / "job.yaml"
    cfg.write_text(_CONFIG_WITH_NAME)

    with (
        patch("gds_cli.session.session_ops.find_session", return_value=None),
        patch("gds_cli.session.session_ops.connect") as connect,
    ):
        result = runner.invoke(sessions_app, ["create", "-f", str(cfg), "--name", "override", "--memory", "8GB"])

    assert result.exit_code == 0, result.output
    session, name = connect.call_args.args[0], connect.call_args.args[1]
    assert name == "override"  # CLI wins over config
    assert session.memory == "8GB"  # CLI wins
    assert session.ttl == timedelta(minutes=30)  # untouched -> from file


def test_sessions_create_requires_a_name() -> None:
    # no --name and no --file with a session.name
    result = runner.invoke(sessions_app, ["create", "--memory", "2GB", "--ttl", "30"])

    assert result.exit_code != 0
    assert "session name required" in result.output


def test_sessions_list_prints_each() -> None:
    info = MagicMock()
    info.name, info.id, info.status, info.memory, info.ttl = "s1", "id-1", "Ready", "2GB", "30m"
    with patch("gds_cli.session.session_ops.list_sessions", return_value=[info]):
        result = runner.invoke(sessions_app, ["list"])

    assert result.exit_code == 0
    assert "s1" in result.output and "id-1" in result.output


def test_sessions_delete_confirmed() -> None:
    with patch("gds_cli.session.session_ops.delete", return_value=True) as delete:
        result = runner.invoke(sessions_app, ["delete", "--name", "s1"], input="y\n")

    assert result.exit_code == 0
    delete.assert_called_once_with("s1")
    assert "Deleted session 's1'." in result.output


def test_sessions_delete_aborts_without_confirmation() -> None:
    with patch("gds_cli.session.session_ops.delete") as delete:
        result = runner.invoke(sessions_app, ["delete", "--name", "s1"], input="n\n")

    assert result.exit_code != 0
    delete.assert_not_called()


def test_sessions_delete_yes_skips_prompt() -> None:
    with patch("gds_cli.session.session_ops.delete", return_value=True) as delete:
        result = runner.invoke(sessions_app, ["delete", "--name", "s1", "--yes"])

    assert result.exit_code == 0
    delete.assert_called_once_with("s1")
