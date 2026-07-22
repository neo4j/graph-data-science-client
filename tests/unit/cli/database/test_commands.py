import json
from pathlib import Path

from gds_cli.database.commands import app
from typer.testing import CliRunner

runner = CliRunner()

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
