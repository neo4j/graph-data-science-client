#!/usr/bin/env python
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

UPSTREAM_REPO = "neo4j/graph-data-science-client"
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Matches the GitHub remote URL forms we expect: git@github.com:o/r,
# ssh://git@github.com/o/r and https://[user@]github.com/o/r, with an
# optional .git suffix and trailing slash.
_GITHUB_URL = re.compile(
    r"^(?:git@|ssh://git@|https?://(?:[^@/]+@)?)github\.com[:/](?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$",
    re.IGNORECASE,
)


def github_owner_repo(url: str) -> str | None:
    """Return the 'owner/repo' for a GitHub remote URL, or None if it is not one."""
    match = _GITHUB_URL.match(url.strip())
    if not match:
        return None

    return f"{match.group('owner')}/{match.group('repo')}".lower()


def list_remotes(repo_dir: Path) -> list[str]:
    result = subprocess.run(["git", "remote"], cwd=repo_dir, check=True, capture_output=True, text=True)
    return result.stdout.split()


def remote_url(repo_dir: Path, remote: str) -> str:
    result = subprocess.run(
        ["git", "remote", "get-url", remote], cwd=repo_dir, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def resolve_upstream_remote(repo_dir: Path = REPO_ROOT) -> str:
    """Find the remote that points at the upstream repository rather than a fork."""
    remotes = list_remotes(repo_dir)
    matches = [remote for remote in remotes if github_owner_repo(remote_url(repo_dir, remote)) == UPSTREAM_REPO]

    if not matches:
        found = ", ".join(remotes) if remotes else "none"
        raise ValueError(
            f"No git remote points at '{UPSTREAM_REPO}'.\n"
            f"Found remotes: {found}.\n"
            "Add the upstream remote and retry:\n"
            f"    git remote add upstream git@github.com:{UPSTREAM_REPO}.git"
        )

    # Prefer 'origin' so the common setup keeps working, otherwise take the first match.
    if "origin" in matches:
        return "origin"

    return matches[0]


def main() -> None:
    try:
        remote = resolve_upstream_remote()
    except ValueError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error

    print(remote)


if __name__ == "__main__":
    main()
