from contextlib import contextmanager
from typing import Iterator


class FeatureNotEnabledError(Exception):
    """Raised when an endpoint is invoked against a session that does not have the feature enabled."""

    def __init__(self, feature: str) -> None:
        super().__init__(
            f"{feature} is not enabled for this session. "
            "Please reach out to the Neo4j GDS team to have it enabled for your session."
        )


@contextmanager
def translate_feature_not_enabled(endpoint: str, feature: str) -> Iterator[None]:
    """Translate the session's "unsupported action" error into a clear feature-not-enabled error."""
    try:
        yield
    except Exception as e:
        message = str(e)
        if "Unsupported action" in message and endpoint in message:
            raise FeatureNotEnabledError(feature) from e
        raise
