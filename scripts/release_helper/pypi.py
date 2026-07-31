from __future__ import annotations

import requests
from packaging.version import Version

PYPI_PACKAGE = "graphdatascience"


def project_url(version: str = "") -> str:
    return f"https://pypi.org/project/{PYPI_PACKAGE}/{version}".rstrip("/") + "/"


def fetch_released_versions() -> set[str]:
    """Return all versions released on PyPI, including pre-releases."""
    resp = requests.get(f"https://pypi.org/pypi/{PYPI_PACKAGE}/json")
    resp.raise_for_status()
    pypi_json = resp.json()

    # `info.version` only ever reports the latest stable version, so use the full release list instead
    return {version for version, files in pypi_json["releases"].items() if files}


def print_latest_released_versions(released_versions: set[str]) -> None:
    versions = sorted((Version(v) for v in released_versions), reverse=True)
    print(f"Latest version released on PyPI: `{versions[0]}`")

    stable_versions = [v for v in versions if not v.is_prerelease]
    if stable_versions and stable_versions[0] != versions[0]:
        print(f"Latest stable version released on PyPI: `{stable_versions[0]}`")


def is_released(version: str, released_versions: set[str]) -> bool:
    return Version(version) in {Version(v) for v in released_versions}


def verify_not_released(version: str) -> None:
    print("Verifying that the version is not released on PyPI yet...")

    released_versions = fetch_released_versions()
    print_latest_released_versions(released_versions)

    if is_released(version, released_versions):
        raise ValueError(
            f"Version {version} is already released on PyPI: {project_url(version)}\n"
            "Bump `__version__` in src/graphdatascience/version.py before releasing."
        )

    print(f"✅ Version {version} is not released on PyPI yet")


def verify_released(version: str) -> None:
    print("Verifying that the version is released on PyPI...")

    released_versions = fetch_released_versions()
    print_latest_released_versions(released_versions)

    if not is_released(version, released_versions):
        raise ValueError(
            f"Version {version} is not released on PyPI yet, see {project_url()}\n"
            "Only run the post-release tasks once the release is published."
        )

    print(f"✅ Version {version} is released on PyPI: {project_url(version)}")
