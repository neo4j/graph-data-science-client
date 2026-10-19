from __future__ import annotations

from enum import StrEnum


class UnknownDatabaseType(Exception):
    """Raised when `SHOW DATABASES` reports a database type the client does not know."""


class InstanceType(StrEnum):
    """
    The type of instance a GDS session is attached to.
    """

    # An AuraDB instance. This is the default and the only type that supports `aura_database_id`.
    DATABASE = "database"
    # A graph engine instance. It is addressed by its instance id and reached through its own uri.
    VIRTUAL_GRAPH = "virtual_graph"

    @classmethod
    def from_show_databases_type(cls, database_type: str) -> InstanceType:
        """
        Map the `type` reported by `SHOW DATABASES` to an `InstanceType`.

        Standard databases are reported as `standard`. Anything that is not a known type raises,
        so that unsupported database types do not silently behave like a database.
        """
        normalized = database_type.strip().lower().replace(" ", "_")
        if normalized == "standard":
            return cls.DATABASE

        try:
            return cls(normalized)
        except ValueError:
            raise UnknownDatabaseType(
                f"Unknown database type `{database_type}` reported by `SHOW DATABASES`."
            ) from None
