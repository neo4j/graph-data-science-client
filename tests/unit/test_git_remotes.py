from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "release_helper" / "git_remotes.py"


def _load_git_remotes() -> ModuleType:
    spec = importlib.util.spec_from_file_location("git_remotes", _MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


git_remotes = _load_git_remotes()


@pytest.mark.parametrize(
    "url, expected",
    [
        ("git@github.com:neo4j/graph-data-science-client.git", "neo4j/graph-data-science-client"),
        ("git@github.com:neo4j/graph-data-science-client", "neo4j/graph-data-science-client"),
        ("ssh://git@github.com/neo4j/graph-data-science-client.git", "neo4j/graph-data-science-client"),
        ("https://github.com/neo4j/graph-data-science-client.git", "neo4j/graph-data-science-client"),
        ("https://github.com/neo4j/graph-data-science-client", "neo4j/graph-data-science-client"),
        ("https://github.com/neo4j/graph-data-science-client/", "neo4j/graph-data-science-client"),
        ("https://user:token@github.com/neo4j/graph-data-science-client.git", "neo4j/graph-data-science-client"),
        ("git@github.com:Neo4j/Graph-Data-Science-Client.git", "neo4j/graph-data-science-client"),
    ],
)
def test_github_owner_repo(url: str, expected: str) -> None:
    assert git_remotes.github_owner_repo(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "git@gitlab.com:neo4j/graph-data-science-client.git",
        "https://gitlab.com/neo4j/graph-data-science-client.git",
        "/local/path/to/repo",
        "not a url",
    ],
)
def test_github_owner_repo_returns_none_for_non_github(url: str) -> None:
    assert git_remotes.github_owner_repo(url) is None


def _git(repo_dir: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo_dir, check=True, capture_output=True)


def test_resolve_prefers_origin(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "remote", "add", "origin", "git@github.com:neo4j/graph-data-science-client.git")
    _git(tmp_path, "remote", "add", "fork", "git@github.com:someuser/graph-data-science-client.git")

    assert git_remotes.resolve_upstream_remote(tmp_path) == "origin"


def test_resolve_finds_upstream_when_origin_is_a_fork(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "remote", "add", "origin", "git@github.com:someuser/gdsclient.git")
    _git(tmp_path, "remote", "add", "upstream", "https://github.com/neo4j/graph-data-science-client.git")

    assert git_remotes.resolve_upstream_remote(tmp_path) == "upstream"


def test_resolve_falls_back_to_first_match(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "remote", "add", "origin", "git@github.com:someuser/gdsclient.git")
    _git(tmp_path, "remote", "add", "zzz", "git@github.com:neo4j/graph-data-science-client.git")
    _git(tmp_path, "remote", "add", "upstream", "git@github.com:neo4j/graph-data-science-client.git")

    assert git_remotes.resolve_upstream_remote(tmp_path) == "upstream"


def test_resolve_ignores_same_named_fork(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "remote", "add", "origin", "git@github.com:TreavVasu/graph-data-science-client.git")

    with pytest.raises(ValueError, match="No git remote points at 'neo4j/graph-data-science-client'"):
        git_remotes.resolve_upstream_remote(tmp_path)


def test_resolve_raises_without_upstream(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "remote", "add", "origin", "git@github.com:someuser/gdsclient.git")

    with pytest.raises(ValueError, match="git remote add upstream git@github.com:neo4j/graph-data-science-client.git"):
        git_remotes.resolve_upstream_remote(tmp_path)
