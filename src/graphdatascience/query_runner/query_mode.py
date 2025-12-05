from enum import Enum

import neo4j


class QueryMode(str, Enum):
    READ = "read"
    WRITE = "write"

    def neo4j_routing(self) -> "neo4j.RoutingControl":
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
