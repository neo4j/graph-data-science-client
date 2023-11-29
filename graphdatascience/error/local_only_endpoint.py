from functools import wraps
from typing import Any, Callable, TypeVar, cast

from ..caller_base import CallerBase

F = TypeVar("F", bound=Callable[..., Any])


def local_projection() -> Callable[[F], F]:
    def decorator(func: F) -> F:
        wraps(func)

        @wraps(func)
        def wrapper(self: CallerBase, *args: Any, **kwargs: Any) -> Any:
            if not self._query_runner.support_local_projections():
                raise RuntimeError(
                    f"Projecting through '{self._namespace}' is not supported."
                    f"Please project the graph using 'gds.graph.project.remoteDb'."
                )

            return func(self, *args, **kwargs)

        return cast(F, wrapper)

    return decorator
