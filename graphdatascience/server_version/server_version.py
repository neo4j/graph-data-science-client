import re
from typing import Type, TypeVar

SV = TypeVar("SV", bound="ServerVersion")


class InvalidServerVersionError(Exception):
    pass


class ServerVersion:
    def __init__(self, major: int, minor: int, patch: int):
        self.major = major
        self.minor = minor
        self.patch = patch

    @classmethod
    def from_string(cls: Type[SV], version: str) -> SV:
        server_version_match = re.search(r"^(\d+)\.(\d+)\.(\d+)", version)
        if not server_version_match:
            raise InvalidServerVersionError(f"{version} is not a valid GDS library version")

        return cls(*map(int, server_version_match.groups()))

    def __lt__(self, other: SV) -> bool:
        if self.major != other.major:
            return self.major < other.major

        if self.minor != other.minor:
            return self.minor < other.minor

        if self.patch != other.patch:
            return self.patch < other.patch

        return False

    def __ge__(self, other: SV) -> bool:
        return not self.__lt__(other)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
