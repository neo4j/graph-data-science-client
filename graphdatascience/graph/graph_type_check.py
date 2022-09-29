from functools import wraps
from typing import Any, Callable, Optional, TypeVar, cast

from .graph_object import Graph

F = TypeVar("F", bound=Callable[..., Any])


def graph_type_check(func: F) -> F:
    @wraps(func)
    def wrapper(self: Any, G: Graph, *args: Any, **kwargs: Any) -> Any:
        if isinstance(G, str):
            raise TypeError(
                f"The parameter 'G' takes a `Graph` object, but received string '{G}'. "
                "To resolve a graph name string into a `Graph` object, please use `gds.graph.get`"
            )

        return func(self, G, *args, **kwargs)

    return cast(F, wrapper)


def graph_type_check_optional(func: F) -> F:
    @wraps(func)
    def wrapper(self: Any, G: Optional[Graph] = None, *args: Any, **kwargs: Any) -> Any:
        if isinstance(G, str):
            raise TypeError(
                f"The parameter 'G' takes a `Graph` object, but received string '{G}'. "
                "To resolve a graph name string into a `Graph` object, please use `gds.graph.get`"
            )

        return func(self, G, *args, **kwargs)

    return cast(F, wrapper)


def from_graph_type_check(func: F) -> F:
    @wraps(func)
    def wrapper(self: Any, graph_name: str, from_G: Graph, *args: Any, **kwargs: Any) -> Any:
        if isinstance(from_G, str):
            raise TypeError(
                f"The parameter 'from_G' takes a `Graph` object, but received string '{from_G}'. "
                "To resolve a graph name string into a `Graph` object, please use `gds.graph.get`"
            )

        return func(self, graph_name, from_G, *args, **kwargs)

    return cast(F, wrapper)
