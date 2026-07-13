from __future__ import annotations

import re


class InvalidServerVersionError(Exception):
    pass


class SemanticVersion:
    """
    A representation of a semantic version, such as for python packages.
    """

    def __init__(self, major: int, minor: int, patch: int | None = None):
        self.major = major
        self.minor = minor

        if patch is None:
            patch = 0
        self.patch = patch

    @classmethod
    def from_string(cls, version: str) -> SemanticVersion:
        server_version_match = re.search(r"^(\d+)\.(\d+)\.?(\d+)?", version)
        if not server_version_match:
            raise InvalidServerVersionError(f"{version} is not a valid semantic version")

        major = int(server_version_match.group(1))
        minor = int(server_version_match.group(2))
        patch = int(server_version_match.group(3)) if server_version_match.group(3) is not None else 0

        return cls(
            major=major,
            minor=minor,
            patch=patch,
        )

    def __lt__(self, other: SemanticVersion) -> bool:
        if self.major != other.major:
            return self.major < other.major

        if self.minor != other.minor:
            return self.minor < other.minor

        if self.patch != other.patch:
            return self.patch < other.patch

        return False

    def __ge__(self, other: SemanticVersion) -> bool:
        return not self.__lt__(other)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
