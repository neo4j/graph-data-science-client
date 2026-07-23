import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from gds_cli.database.commands import app
from gds_cli.database.db import DeleteStats
from typer.testing import CliRunner

runner = CliRunner()


def _mock_client(nodes: int = 3, rels: int = 2) -> MagicMock:
    """A DatabaseClient whose delete() returns fixed counts (never touches a DB)."""
    client = MagicMock()
    client.delete.return_value = DeleteStats(nodes=nodes, relationships=rels)
    return client


CONSTRUCT_GRAPH = {
    "nodes": [[{"nodeId": 0, "labels": "Node", "score": 1.0}]],
    "relationships": [[{"sourceNodeId": 0, "targetNodeId": 0, "relationshipType": "REL"}]],
}


def _write_graph_file(tmp_path: Path) -> str:
    path = tmp_path / "graph.json"
    path.write_text(json.dumps(CONSTRUCT_GRAPH))
    return str(path)


def test_upload_dry_run_does_not_touch_database(tmp_path: Path) -> None:
    file = _write_graph_file(tmp_path)

    result = runner.invoke(app, ["upload", "--file", file, "--dry-run"])

    assert result.exit_code == 0
    assert "Dry run: nothing written." in result.output


def test_upload_multiple_files_dry_run(tmp_path: Path) -> None:
    a = tmp_path / "a.json"
    a.write_text(json.dumps(CONSTRUCT_GRAPH))
    b = tmp_path / "b.json"
    b.write_text(json.dumps(CONSTRUCT_GRAPH))

    result = runner.invoke(app, ["upload", "-f", str(a), "-f", str(b), "--dry-run"])

    assert result.exit_code == 0
    assert "a.json" in result.output
    assert "b.json" in result.output


def test_upload_directory_dry_run(tmp_path: Path) -> None:
    (tmp_path / "a.json").write_text(json.dumps(CONSTRUCT_GRAPH))
    (tmp_path / "b.json").write_text(json.dumps(CONSTRUCT_GRAPH))
    (tmp_path / "notes.txt").write_text("ignored")

    result = runner.invoke(app, ["upload", "-f", str(tmp_path), "--dry-run"])

    assert result.exit_code == 0
    assert "a.json" in result.output
    assert "b.json" in result.output
    assert "notes.txt" not in result.output


def test_upload_empty_directory_errors(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()

    result = runner.invoke(app, ["upload", "-f", str(empty), "--dry-run"])

    assert result.exit_code == 1
    assert "no graph files" in result.output


def test_upload_requires_file() -> None:
    result = runner.invoke(app, ["upload"])

    assert result.exit_code != 0
    assert "--file" in result.output


def test_upload_unknown_file_exits_nonzero() -> None:
    result = runner.invoke(app, ["upload", "--file", "does-not-exist.json", "--dry-run"])

    assert result.exit_code == 1
    assert "Error:" in result.output


def test_delete_requires_all_or_label() -> None:
    result = runner.invoke(app, ["delete"])

    assert result.exit_code != 0
    assert "Pass --all or --label" in result.output


def test_delete_aborts_when_not_confirmed() -> None:
    client = _mock_client(nodes=3, rels=2)
    with patch("gds_cli.database.commands._client", return_value=client):
        result = runner.invoke(app, ["delete", "--all"], input="n\n")

    assert result.exit_code != 0  # aborted
    assert "Deleted" not in result.output
    # only the dry-run preview ran; no destructive delete
    assert client.delete.call_count == 1
    assert client.delete.call_args.kwargs.get("dry_run") is True


def test_delete_proceeds_when_confirmed() -> None:
    client = _mock_client(nodes=3, rels=2)
    with patch("gds_cli.database.commands._client", return_value=client):
        result = runner.invoke(app, ["delete", "--all"], input="y\n")

    assert result.exit_code == 0
    assert "Deleted 3 nodes and 2 relationships" in result.output
    # preview (dry_run=True) then the real delete (dry_run defaults to False)
    assert client.delete.call_count == 2
    assert client.delete.call_args_list[0].kwargs.get("dry_run") is True
    assert client.delete.call_args_list[-1].kwargs.get("dry_run") is not True


def test_delete_yes_skips_prompt() -> None:
    client = _mock_client(nodes=1, rels=0)
    with patch("gds_cli.database.commands._client", return_value=client):
        result = runner.invoke(app, ["delete", "--all", "--yes"])  # no stdin needed

    assert result.exit_code == 0
    assert "Deleted 1 nodes" in result.output
    assert client.delete.call_count == 2


def test_delete_nothing_when_empty_does_not_prompt() -> None:
    client = _mock_client(nodes=0, rels=0)
    with patch("gds_cli.database.commands._client", return_value=client):
        result = runner.invoke(app, ["delete", "--all"])  # would hang on prompt if it asked

    assert result.exit_code == 0
    assert "Nothing to delete" in result.output
    assert client.delete.call_count == 1  # preview only


def test_delete_dry_run_reports_without_deleting() -> None:
    client = _mock_client(nodes=5, rels=4)
    with patch("gds_cli.database.commands._client", return_value=client):
        result = runner.invoke(app, ["delete", "--all", "--dry-run"])

    assert result.exit_code == 0
    assert "Would delete 5 nodes and 4 relationships" in result.output
    assert client.delete.call_count == 1
    assert client.delete.call_args.kwargs.get("dry_run") is True
