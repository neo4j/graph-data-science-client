#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

import gha_workflows

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def main() -> None:
    # Release branches have no doc/publish.yml, so allow an explicit version there.
    versions = [sys.argv[1]] if len(sys.argv) > 1 else gha_workflows.publish_branches(REPO_ROOT)

    print(f"Syncing docs workflow branch lists with {versions}")
    for version in versions:
        gha_workflows.add_branch_to_triggers(REPO_ROOT, version)


if __name__ == "__main__":
    main()
