#!/usr/bin/env python
from __future__ import annotations

import re
import sys
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

    def major_minor_suffix(self) -> str:
        return f"{self.major}.{self.minor}{self.suffix}"

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

    updated = re.sub(VERSION_REGEX, f'__version__ = "{new_version.major_minor_suffix()}"', content)

    if updated == content:
        print(f"☑️ No changes needed for {VERSION_FILE}")
        return

    VERSION_FILE.write_text(updated)
    print(f"✅ Updated {VERSION_FILE.relative_to(REPO_ROOT)} to version {new_version}")


def update_changelog(new_version: PythonLibraryVersion) -> None:
    changelog_file = REPO_ROOT / "changelog.md"

    template = Path(__file__).parent / "changelog.template"
    new_changelog_body = template.read_text().replace("<VERSION>", str(new_version))

    if changelog_file.read_text() == new_changelog_body:
        print(f"☑️ No changes needed for {changelog_file.relative_to(REPO_ROOT)}")
        return

    changelog_file.write_text(new_changelog_body)
    print(f"✅ Updated {changelog_file.relative_to(REPO_ROOT)} for version {new_version}")


def update_publish_yml(released_version: PythonLibraryVersion, next_version: PythonLibraryVersion) -> None:
    publish_file = REPO_ROOT / "doc" / "publish.yml"
    content = publish_file.read_text()

    # Extract major.minor from released version
    new_branch = f"{released_version.major_minor()}"

    def update_branches(branches: str) -> str:
        if new_branch in branches:
            return branches
        else:
            return f"{branches}, '{new_branch}'"

    # Update branches list
    updated = re.sub(r"(branches:\s*\[)([^\]]*)", lambda m: f"{m.group(1)}{update_branches(m.group(2))}", content)
    # Update api-version to next version
    updated = re.sub(r"api-version:\s*\d+\.\d+", f"api-version: {next_version.major_minor()}", updated)

    if updated == content:
        print(f"☑️ No changes needed for {publish_file.relative_to(REPO_ROOT)}")
        return

    publish_file.write_text(updated)
    print(
        f"✅ Updated {publish_file.relative_to(REPO_ROOT)} - added branch '{new_branch}' and set api-version to {next_version.major_minor()}"
    )


def update_preview_yml(released_version: PythonLibraryVersion) -> None:
    preview_file = REPO_ROOT / "doc" / "preview.yml"
    content = preview_file.read_text()

    updated = re.sub(r"api-version: (\d+)\.(\d+)", f"api-version: {released_version.major_minor()}", content)

    if updated == content:
        print(f"☑️ No changes needed for {preview_file.relative_to(REPO_ROOT)}")
        return

    preview_file.write_text(updated)
    print(f"✅ Updated {preview_file.relative_to(REPO_ROOT)} to version {released_version}")


def update_antora_yml(next_version: PythonLibraryVersion) -> None:
    antora_file = REPO_ROOT / "doc" / "antora.yml"
    content = antora_file.read_text()

    updated = re.sub(r"version: '[^']*'", f"version: '{next_version}'", content)
    updated = re.sub(r"docs-version: '[^']*'", f"docs-version: '{next_version}'", updated)

    if updated == content:
        print(f"☑️ No changes needed for {antora_file.relative_to(REPO_ROOT)}")
        return

    antora_file.write_text(updated)
    print(f"✅ Updated {antora_file.relative_to(REPO_ROOT)} to version {next_version}")


def update_package_json(new_version: PythonLibraryVersion) -> None:
    """Update version in package.json."""
    package_file = REPO_ROOT / "doc" / "package.json"
    content = package_file.read_text()

    # Set to preview version
    preview_version = f"{new_version.major_minor()}-preview"
    updated = re.sub(r'"version":\s*"[^"]*"', f'"version": "{preview_version}"', content)

    if updated == content:
        print(f"☑️ No changes needed for {package_file.relative_to(REPO_ROOT)}")
        return

    package_file.write_text(updated)
    print(f"✅ Updated {package_file.relative_to(REPO_ROOT)} to version {preview_version}")


def update_installation_adoc(next_version: PythonLibraryVersion) -> None:
    new_compat_table_entry = f"""
.1+<.^| {next_version.major_minor()}
.1+<.^| >= 2.6, < 2.24
.1+<.^| >= 3.10, < 3.14
.1+<.^| >= 4.4.12, < 7.0.0
    """.strip()

    installation_file = REPO_ROOT / "doc" / "modules" / "ROOT" / "pages" / "installation.adoc"
    content = installation_file.read_text()

    version_table_regex = r"(?s)(\| Python Client \| GDS version[^\n]*)(.*)(\|===)"
    match = re.search(version_table_regex, content, re.MULTILINE)
    if not match:
        raise ValueError("Could not find installation table in installation.adoc")
    version_table = match.group(2)

    if new_compat_table_entry in version_table:
        print(f"☑️ No changes needed for {installation_file.relative_to(REPO_ROOT)}")
        return

    updated_version_table = f"\n{new_compat_table_entry}\n" + version_table
    updated = content.replace(version_table, updated_version_table)
    installation_file.write_text(updated)
    print(f"✅ Updated {installation_file.relative_to(REPO_ROOT)} with new version {next_version}")


def main() -> None:
    # Get current version
    released_version = read_library_version()

    # read new version from args

    print(f"Released version: {released_version}")

    # Calculate next version if not provided
    next_version = (
        PythonLibraryVersion.from_string(sys.argv[1]) if len(sys.argv) > 1 else bump_version(released_version)
    )
    print(f"Next version: {next_version}")

    print("\nStarting post-release tasks...")

    update_version_py(next_version)

    if not released_version.is_alpha():
        update_changelog(next_version)

        update_package_json(next_version)

        update_antora_yml(next_version)
        update_publish_yml(released_version, next_version)
        update_preview_yml(released_version)

        update_installation_adoc(next_version)

    print("\n✅ Post-release tasks completed!")
    print("\nNext steps:")
    print("* Review the changes")
    if not released_version.is_alpha():
        print("* Update installation.adoc")
    print(f"* Commit with message: 'Prepare for {next_version} development'")
    print("* Push to main branch")


if __name__ == "__main__":
    main()
