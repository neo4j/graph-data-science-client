from __future__ import annotations

from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.versions import ServerVersion

# The `resultAsVector` config key only exists on GDS 2026.11 and later.
MIN_RESULT_AS_VECTOR_VERSION = ServerVersion(2026, 11, 0)


def resolve_result_as_vector(query_runner: QueryRunner, result_as_vector: bool) -> bool | None:
    """Resolve the `resultAsVector` config value for the connected server.

    The key is unknown to older servers, so it is omitted there to keep existing calls working.
    Requesting it on an older server raises instead of failing server-side with an unknown key.
    """
    if query_runner.server_version() >= MIN_RESULT_AS_VECTOR_VERSION:
        return result_as_vector

    if result_as_vector:
        raise ValueError(
            f"result_as_vector requires GDS {MIN_RESULT_AS_VECTOR_VERSION} or later, "
            f"but the connected server is {query_runner.server_version()}."
        )

    return None
