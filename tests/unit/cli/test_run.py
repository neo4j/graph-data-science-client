from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from gds_cli.run import _load_configs, _resolve_overwrite, run
from gds_cli.session.config import ComputeSpec, JobsConfig, JobSpec, ProjectSpec, SessionConfig

_CONFIG_YAML = (
    "session:\n"
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


def test_resolve_overwrite_true_short_circuits_without_checking() -> None:
    gds = MagicMock()

    assert _resolve_overwrite(gds, _cfg(), "my-session", overwrite_graph=True) is True
    gds.graph.exists.assert_not_called()


def test_resolve_overwrite_false_when_no_graphs_exist() -> None:
    gds = MagicMock()
    gds.graph.exists.return_value = False

    assert _resolve_overwrite(gds, _cfg(), "my-session", overwrite_graph=False) is False


def test_resolve_overwrite_prompts_and_confirms_drop() -> None:
    gds = MagicMock()
    gds.graph.exists.return_value = True  # graph left over from a prior run

    with patch("typer.confirm", return_value=True) as confirm:
        result = _resolve_overwrite(gds, _cfg(), "my-session", overwrite_graph=False)

    confirm.assert_called_once()
    assert result is True


def test_resolve_overwrite_aborts_when_user_declines() -> None:
    gds = MagicMock()
    gds.graph.exists.return_value = True

    with patch("typer.confirm", return_value=False):
        with pytest.raises(typer.Exit):
            _resolve_overwrite(gds, _cfg(), "my-session", overwrite_graph=False)


def test_resolve_overwrite_assume_yes_drops_without_prompting() -> None:
    gds = MagicMock()
    gds.graph.exists.return_value = True  # leftover graph, but no TTY to prompt on

    with patch("typer.confirm") as confirm:
        result = _resolve_overwrite(gds, _cfg(), "my-session", overwrite_graph=False, assume_yes=True)

    confirm.assert_not_called()  # --yes answers affirmatively without asking
    assert result is True


def test_load_configs_expands_directory_sorted(tmp_path: Path) -> None:
    (tmp_path / "b.yaml").write_text(_CONFIG_YAML)
    (tmp_path / "a.yaml").write_text(_CONFIG_YAML)
    (tmp_path / "notes.txt").write_text("ignored")

    configs = _load_configs([str(tmp_path)], None)

    assert [name for name, _, _ in configs] == ["a.yaml", "b.yaml"]  # sorted, .txt skipped
    assert all(base_dir == str(tmp_path) for _, base_dir, _ in configs)


def test_load_configs_empty_directory_errors(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()

    with pytest.raises(typer.Exit):
        _load_configs([str(tmp_path / "empty")], None)


def test_run_directory_runs_each_config_independently(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text(_CONFIG_YAML)
    (tmp_path / "b.yaml").write_text(_CONFIG_YAML)

    with (
        patch("gds_cli.run._connect") as connect,
        patch("gds_cli.session.steps.run_all") as run_all,
        patch("gds_cli.session.session_ops.delete") as delete,
    ):
        connect.return_value.graph.exists.return_value = False  # no overwrite prompt

        run(files=[str(tmp_path)], session_name=None, overwrite_graph=False, progress_bar=False, env_file=None)

    # each config is self-contained: its own connect, its own run_all, its own throwaway
    # session deleted afterwards - exactly like `gds run -f <file>` per file.
    assert connect.call_count == 2
    assert run_all.call_count == 2
    assert delete.call_count == 2
    # distinct throwaway session names, one per config
    names = [c.args[1] for c in connect.call_args_list]
    assert names[0] != names[1]
    assert all(n.startswith("cli-") for n in names)
    assert all(c.kwargs["base_dir"] == str(tmp_path) for c in run_all.call_args_list)


def test_run_directory_with_session_name_reuses_and_keeps(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text(_CONFIG_YAML)
    (tmp_path / "b.yaml").write_text(_CONFIG_YAML)

    with (
        patch("gds_cli.run._connect") as connect,
        patch("gds_cli.session.steps.run_all"),
        patch("gds_cli.session.session_ops.delete") as delete,
    ):
        connect.return_value.graph.exists.return_value = False

        run(files=[str(tmp_path)], session_name="warm", overwrite_graph=False, progress_bar=False, env_file=None)

    # every config reconnects to the same named session, and none is deleted
    assert connect.call_count == 2
    assert all(c.args[1] == "warm" for c in connect.call_args_list)
    delete.assert_not_called()


def test_run_single_file_keeps_session_with_name(tmp_path: Path) -> None:
    (tmp_path / "job.yaml").write_text(_CONFIG_YAML)

    with (
        patch("gds_cli.run._connect") as connect,
        patch("gds_cli.session.steps.run_all") as run_all,
        patch("gds_cli.session.session_ops.delete") as delete,
    ):
        connect.return_value.graph.exists.return_value = False

        run(
            files=[str(tmp_path / "job.yaml")],
            session_name="warm",
            overwrite_graph=False,
            progress_bar=False,
            env_file=None,
        )

    connect.assert_called_once()
    assert connect.call_args.args[1] == "warm"  # explicit session name used
    delete.assert_not_called()  # --session-name => kept
    assert run_all.call_count == 1


def test_run_repeated_file_flags_run_each(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text(_CONFIG_YAML)
    (tmp_path / "b.yaml").write_text(_CONFIG_YAML)

    with (
        patch("gds_cli.run._connect") as connect,
        patch("gds_cli.session.steps.run_all") as run_all,
        patch("gds_cli.session.session_ops.delete"),
    ):
        connect.return_value.graph.exists.return_value = False

        # repeated -f: both configs run (previously only the last survived)
        run(
            files=[str(tmp_path / "a.yaml"), str(tmp_path / "b.yaml")],
            session_name=None,
            overwrite_graph=False,
            progress_bar=False,
            env_file=None,
        )

    assert run_all.call_count == 2


def test_load_configs_dedupes_and_preserves_order(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text(_CONFIG_YAML)
    (tmp_path / "b.yaml").write_text(_CONFIG_YAML)

    # b first, then the whole directory: b must not be run twice, order preserved.
    configs = _load_configs([str(tmp_path / "b.yaml"), str(tmp_path)], None)

    assert [name for name, _, _ in configs] == ["b.yaml", "a.yaml"]
