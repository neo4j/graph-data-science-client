from __future__ import annotations

from graphdatascience.graph.graph_api import Graph


def validate_distinct_from_source(graph_name: str, source_graph: Graph) -> None:
    """Raise ``ValueError`` if the target graph name equals the source graph's name.

    Creating a derived graph (e.g. a filter or sample) with the same name as the graph it
    is derived from would overwrite the source mid-operation.
    """
    source_name = source_graph.name()
    if graph_name == source_name:
        raise ValueError(
            f"The target graph name '{graph_name}' must not equal the source graph name '{source_name}', "
            "as this would overwrite the graph being read from."
        )
