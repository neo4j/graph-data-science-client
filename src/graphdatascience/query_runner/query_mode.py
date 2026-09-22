from __future__ import annotations

from enum import Enum
from typing import Literal

import neo4j


class QueryMode(str, Enum):
    """
    The mode in which a Cypher query is run.

    Plain strings ("read"/"write") are accepted wherever a `QueryMode` is expected.
    """

    READ = "read"
    WRITE = "write"

    @classmethod
    def of(cls, mode: QueryMode | str) -> QueryMode:
        """
        Normalize a `QueryMode` or a plain string into a `QueryMode`.
        """
        if isinstance(mode, QueryMode):
            return mode

        valid = [m.value for m in cls]
        if mode not in valid:
            raise ValueError(f"Invalid query mode: '{mode}'. Valid values are: {valid}.")

        return cls(mode)

    def neo4j_routing(self) -> neo4j.RoutingControl:
        if self == QueryMode.READ:
            return neo4j.RoutingControl.READ
        elif self == QueryMode.WRITE:
            return neo4j.RoutingControl.WRITE
        else:
            raise ValueError(f"Unknown query mode: {self}")

    def neo4j_access_mode(self) -> str:
        if self == QueryMode.READ:
            return neo4j.READ_ACCESS
        elif self == QueryMode.WRITE:
            return neo4j.WRITE_ACCESS
        else:
            raise ValueError(f"Unknown query mode: {self}")


QueryModeLike = QueryMode | Literal["read", "write"]
