#!/usr/bin/env python
from __future__ import annotations

import re
from pathlib import Path

# These workflow files trigger on the release branches listed in doc/publish.yml. Each
# branch needs its own version in the copies that live on that branch, so a new release
# branch adds itself here when it is cut.
WORKFLOW_FILES = [
    "docs-generate-html.yml",
    "docs-pr-checks.yml",
    "docs-api-reference.yml",
    "docs-teardown.yml",
    "docs-api-reference-teardown.yml",
]

_BRANCHES_BLOCK = re.compile(r"(?m)^(?P<indent>[ \t]+)branches:[ \t]*\n(?P<entries>(?:[ \t]+-[^\n]*\n)*)")
_PUBLISH_BRANCHES = re.compile(r"(?m)^[ \t]+branches:[ \t]*\[([^\]]*)\]")


def _workflow_path(repo_root: Path, workflow: str) -> Path:
    return repo_root / ".github" / "workflows" / workflow


def _unquote(value: str) -> str:
    return value.strip().strip("'\"")


def _parse_workflow_branches(content: str) -> list[str]:
    match = _BRANCHES_BLOCK.search(content)
    if match is None:
        raise ValueError("Could not find a 'branches:' list")

    return [_unquote(line.strip()[1:]) for line in match.group("entries").splitlines()]


def workflow_branches(repo_root: Path, workflow: str) -> list[str]:
    content = _workflow_path(repo_root, workflow).read_text()
    return _parse_workflow_branches(content)


def publish_branches(repo_root: Path) -> list[str]:
    publish_file = repo_root / "doc" / "publish.yml"
    content = publish_file.read_text()

    match = _PUBLISH_BRANCHES.search(content)
    if match is None:
        raise ValueError(f"Could not find a 'branches:' list in {publish_file}")

    entries = [_unquote(entry) for entry in match.group(1).split(",")]
    # 'HEAD' is main, which is not a release branch.
    return [entry for entry in entries if entry and entry != "HEAD"]


def _add_branch_to_content(content: str, version: str) -> str:
    match = _BRANCHES_BLOCK.search(content)
    if match is None:
        raise ValueError("Could not find a 'branches:' list")

    entries = match.group("entries")
    quoted_branch = f"'{version}'"
    if any(line.strip() == f"- {quoted_branch}" for line in entries.splitlines()):
        return content

    if entries:
        last_entry = entries.splitlines()[-1]
        entry_indent = last_entry[: len(last_entry) - len(last_entry.lstrip())]
        insert_at = match.end("entries")
    else:
        entry_indent = match.group("indent") + "  "
        insert_at = match.end()

    return f"{content[:insert_at]}{entry_indent}- {quoted_branch}\n{content[insert_at:]}"


def add_branch_to_triggers(repo_root: Path, version: str) -> None:
    for workflow in WORKFLOW_FILES:
        path = _workflow_path(repo_root, workflow)
        content = path.read_text()

        try:
            updated = _add_branch_to_content(content, version)
        except ValueError as error:
            raise ValueError(f"{path.relative_to(repo_root)}: {error}") from error

        if updated == content:
            print(f"☑️ No changes needed for {path.relative_to(repo_root)}")
            continue

        path.write_text(updated)
        print(f"✅ Added branch '{version}' to {path.relative_to(repo_root)}")
