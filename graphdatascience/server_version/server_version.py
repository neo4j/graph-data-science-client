class ServerVersion:
    def __init__(self, major: int, minor: int, patch: int):
        self.major = major
        self.minor = minor
        self.patch = patch

    def __lt__(self, other: "ServerVersion") -> bool:
        if self.major != other.major:
            return self.major < other.major

        if self.minor != other.minor:
            return self.minor < other.minor

        if self.patch != other.patch:
            return self.patch < other.patch

        return False

    def __ge__(self, other: "ServerVersion") -> bool:
        return not self.__lt__(other)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
