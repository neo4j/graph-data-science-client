#!/usr/bin/env python
from __future__ import annotations

import re
from pathlib import Path

from graphdatascience.semantic_version.semantic_version import SemanticVersion

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
VERSION_FILE = REPO_ROOT / "src" / "graphdatascience" / "version.py"

VERSION_REGEX = r'__version__ = "([^"]*)"'


class PythonLibraryVersion:
    def __init__(self, major: int, minor: int, patch: int, suffix: str = ""):
        self.major = major
        self.minor = minor
        self.patch = patch
        self.suffix = suffix

    @classmethod
    def from_string(cls, version: str) -> PythonLibraryVersion:
        parts = version.split("a")
        if len(parts) == 2:
            version, alpha_version = parts
            suffix = f"a{alpha_version}"
        else:
            version = parts[0]
            suffix = ""

        semantic_version = SemanticVersion.from_string(version)
        return cls(semantic_version.major, semantic_version.minor, semantic_version.patch, suffix)

    def is_alpha(self) -> bool:
        return self.suffix.startswith("a")

    def alpha(self) -> int | None:
        if self.is_alpha():
            return int(self.suffix[1:])
        return None

    def major_minor(self) -> str:
        return f"{self.major}.{self.minor}"

    def copy(self) -> PythonLibraryVersion:
        return PythonLibraryVersion(
            self.major,
            self.minor,
            self.patch,
            self.suffix,
        )

    def __str__(self) -> str:
        if self.patch > 0:
            return f"{self.major}.{self.minor}.{self.patch}{self.suffix}"
        else:
            return f"{self.major}.{self.minor}{self.suffix}"


def read_library_version() -> PythonLibraryVersion:
    version_file = VERSION_FILE.read_text()
    version_regex = r'^__version__\s*=\s*"([^"]*)"'

    match = re.search(version_regex, version_file)
    if not match:
        raise ValueError("Could not find version string in version.py")

    return PythonLibraryVersion.from_string(match.group(1))


def bump_version(current_version: PythonLibraryVersion) -> PythonLibraryVersion:
    new_version = current_version.copy()
    if alpha_version := current_version.alpha():
        # Alpha release - bump alpha path
        next_alpha = alpha_version + 1
        new_version.suffix = f"a{next_alpha}"
    else:
        new_version.minor += 1
    return new_version


def update_version_py(new_version: PythonLibraryVersion) -> None:
    content = VERSION_FILE.read_text()

    updated = re.sub(VERSION_REGEX, f'__version__ = "{new_version.major_minor()}"', content)

    VERSION_FILE.write_text(updated)
    print(f"✅ Updated {VERSION_FILE} to version {new_version}")


def update_changelog(new_version: PythonLibraryVersion) -> None:
    changelog_file = REPO_ROOT / "changelog.md"

    template = Path(__file__).parent / "changelog.template"
    new_changelog_body = template.read_text().replace("<VERSION>", str(new_version))

    changelog_file.write_text(new_changelog_body)

    print(f"✅ Updated {changelog_file} for version {new_version}")


def update_publish_yml(repo_root: Path, released_version: PythonLibraryVersion) -> None:
    """Add new release branch to publish.yml."""
    publish_file = repo_root / "doc" / "publish.yml"
    content = publish_file.read_text()

    # Extract major.minor from released version
    new_branch = f"{released_version.major_minor()}"

    # Update branches list
    updated = re.sub(r"(branches:\s*\[)([^\]]*)", lambda m: f"{m.group(1)}{m.group(2)}, '{new_branch}'", content)

    publish_file.write_text(updated)
    print(f"✓ Updated {publish_file.relative_to(repo_root)} - added branch '{new_branch}'")


def update_antora_yml(repo_root: Path, new_version: str) -> None:
    """Update version in antora.yml."""
    antora_file = repo_root / "doc" / "antora.yml"
    content = antora_file.read_text()

    updated = re.sub(r"version: '[^']*'", f"version: '{new_version}'", content)
    updated = re.sub(r"docs-version: '[^']*'", f"docs-version: '{new_version}'", updated)

    antora_file.write_text(updated)
    print(f"✓ Updated {antora_file.relative_to(repo_root)} to version {new_version}")


def update_package_json(repo_root: Path, new_version: str) -> None:
    """Update version in package.json."""
    package_file = repo_root / "doc" / "package.json"
    content = package_file.read_text()

    # Set to preview version
    preview_version = f"{new_version}-preview"
    updated = re.sub(r'"version":\s*"[^"]*"', f'"version": "{preview_version}"', content)

    package_file.write_text(updated)
    print(f"✓ Updated {package_file.relative_to(repo_root)} to version {preview_version}")


def main() -> None:
    # Get current version
    current_version = read_library_version()

    print(f"Current version: {current_version}")

    # Calculate next version
    next_version = bump_version(current_version)
    print(f"Next version: {next_version}")

    print("\nStarting post-release tasks...")

    update_version_py(next_version)

    if not current_version.is_alpha():
        update_changelog(next_version)
        # update_publish_yml(current_version)
        # update_antora_yml(next_version)
        # TODO update preview.yml
        # update_package_json(next_version)

        # update_installation_adoc(current_version, next_version)

    print("\n✅ Post-release tasks completed!")
    print("\nNext steps:")
    print("* Review the changes")
    if not current_version.is_alpha():
        print("* Update installation.adoc")
    print(f"* Commit with message: 'Prepare for {next_version} development'")
    print("* Push to main branch")


if __name__ == "__main__":
    main()
