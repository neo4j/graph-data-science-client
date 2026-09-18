#!/usr/bin/env python
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
VERSION_FILE = REPO_ROOT / "src" / "graphdatascience" / "version.py"


def read_library_version() -> str:
    version_file = VERSION_FILE.read_text()
    version_regex = r'^__version__\s*=\s*"([^"]*)"'

    match = re.search(version_regex, version_file)
    if not match:
        raise ValueError("Could not find version string in version.py")

    return match.group(1)


def release_version(version: str) -> str:
    """Reduce a version string to its major.minor docs version, dropping any suffix."""
    match = re.match(r"(\d+\.\d+)", version)
    if not match:
        raise ValueError(f"Could not parse version string '{version}'")

    return match.group(1)


def update_antora_yml(version: str) -> None:
    antora_file = REPO_ROOT / "doc" / "antora.yml"
    content = antora_file.read_text()

    updated = re.sub(r"(?m)^version:\s*'[^']*'\s*$", f"version: '{version}'", content)
    updated = re.sub(r"docs-version:\s*'[^']*'", f"docs-version: '{version}'", updated)
    updated = re.sub(r"(?m)^prerelease:.*\n?", "", updated)

    if updated == content:
        print(f"☑️ No changes needed for {antora_file.relative_to(REPO_ROOT)}")
        return

    antora_file.write_text(updated)
    print(f"✅ Updated {antora_file.relative_to(REPO_ROOT)} to version {version}")


def update_package_json(version: str) -> None:
    package_file = REPO_ROOT / "doc" / "package.json"
    content = package_file.read_text()

    updated = re.sub(r'"version":\s*"[^"]*"', f'"version": "{version}"', content)

    if updated == content:
        print(f"☑️ No changes needed for {package_file.relative_to(REPO_ROOT)}")
        return

    package_file.write_text(updated)
    print(f"✅ Updated {package_file.relative_to(REPO_ROOT)} to version {version}")


def update_preview_yml(version: str) -> None:
    preview_file = REPO_ROOT / "doc" / "preview.yml"
    content = preview_file.read_text()

    updated = re.sub(r"api-version:\s*[\d.]+(?:-\w+)?", f"api-version: {version}", content)

    if updated == content:
        print(f"☑️ No changes needed for {preview_file.relative_to(REPO_ROOT)}")
        return

    preview_file.write_text(updated)
    print(f"✅ Updated {preview_file.relative_to(REPO_ROOT)} to version {version}")


def main() -> None:
    raw_version = sys.argv[1] if len(sys.argv) > 1 else read_library_version()
    version = release_version(raw_version)

    print(f"Preparing docs for release `{version}`")

    update_antora_yml(version)
    update_package_json(version)
    update_preview_yml(version)

    print("\n✅ Release branch docs prepared!")


if __name__ == "__main__":
    main()
