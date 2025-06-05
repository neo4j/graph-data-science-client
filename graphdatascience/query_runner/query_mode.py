from enum import Enum

import neo4j
import neo4j.routing


class QueryMode(str, Enum):
    READ = "read"
    WRITE = "write"

    def neo4j_mode(self) -> neo4j.RoutingControl:
        if self == QueryMode.READ:
            return neo4j.RoutingControl.READ
        elif self == QueryMode.WRITE:
            return neo4j.RoutingControl.WRITE
        else:
            raise ValueError(f"Unknown query mode: {self}")
