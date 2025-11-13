import inspect
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, get_type_hints

import pytest

from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints


def load_parameter_descriptions() -> Dict[str, list[str] | str]:
    """Load the canonical parameter descriptions from parameters.json."""
    # Option 1: Relative to current test file
    current_file = Path(__file__)
    params_file = current_file.parent / "resources" / "parameters.json"

    if not params_file.exists():
        return {}

    with open(params_file, "r") as f:
        return json.load(f)  # type: ignore


def extract_param_descriptions(docstring: str | None) -> Dict[str, str]:
    """Extract parameter descriptions from a docstring.

    Returns a dict mapping parameter names to their descriptions.
    """
    if not docstring:
        return {}

    descriptions = {}
    # Match parameter sections
    param_pattern = r"^\s*(\w+)\s*(?:[:\(].*?)?\n\s+(.*?)(?=^\s*\w+\s*(?:[:\(]|\n\s+)|\Z)"

    # Find the Parameters section
    params_section = re.search(
        r"Parameters\s*\n\s*-+\s*\n(.*?)(?=Returns|Raises|\Z)", docstring, re.DOTALL | re.MULTILINE
    )

    if params_section:
        params_text = params_section.group(1)
        for match in re.finditer(param_pattern, params_text, re.MULTILINE | re.DOTALL):
            param_name = match.group(1)
            param_desc = match.group(2).strip()
            # Clean up the description
            param_desc = re.sub(r"\s+", " ", param_desc)
            descriptions[param_name] = param_desc

    return descriptions


def is_gds_type(obj: Any) -> bool:
    if not hasattr(obj, "__module__"):
        return False
    module = obj.__module__
    return module and module.startswith("graphdatascience")  # type: ignore


def get_all_endpoint_methods(session_class: type, prefix: str = "", visited: set[int] | None = None) -> Dict[str, Any]:
    """Recursively get all endpoint methods from SessionV2Endpoints properties.

    Args:
        session_class: The class to extract methods from
        prefix: The current path prefix (e.g., "knn.filtered")
        visited: Set of already visited classes to avoid infinite recursion
    """
    if visited is None:
        visited = set()

    # Avoid infinite recursion
    if id(session_class) in visited:
        return {}
    visited.add(id(session_class))

    methods = {}

    # Get all attributes
    for attr_name in dir(session_class):
        if attr_name.startswith("_") and attr_name not in ["__call__"]:
            continue

        attr = getattr(session_class, attr_name)

        # Handle properties (traverse them further)
        if isinstance(attr, property):
            prop_func = attr.fget
            if not prop_func:
                continue

            hints = get_type_hints(prop_func)
            return_annotation = hints.get("return")

            if return_annotation and return_annotation is not type(None):
                current_prefix = f"{prefix}.{attr_name}" if prefix else attr_name

                # If it's a GDS type, recurse into it
                if is_gds_type(return_annotation):
                    nested_methods = get_all_endpoint_methods(return_annotation, current_prefix, visited)
                    methods.update(nested_methods)

        # Handle regular methods
        elif callable(attr) and not isinstance(attr, type):
            if inspect.ismethod(attr) or inspect.isfunction(attr):
                full_name = f"{prefix}.{attr_name}" if prefix else attr_name
                methods[full_name] = attr

    return methods


def test_common_parameter_consistency() -> None:
    """Test that common parameters have consistent descriptions across all endpoints."""

    # Common parameters that should have the same description everywhere
    common_params = {
        "G: GraphV2",
        "relationship_types: list[str]",
        "node_labels: list[str]",
        "sudo: bool",
        "job_id: str | None",
        "random_seed: int | None",
        "concurrency: int | None",
        "write_concurrency: int | None",
        "mutate_property: str",
        "write_property: str",
        "consecutive_ids: bool",
        "scaler: str | dict[str, str | int | float] | ScalerConfig",
        "log_progress: bool",
        "max_iterations: int",
        # "G: GraphV2 | dict[str, Any]",
        # "tolerance: float",
        # "source_node: int",
        "relationship_weight_property: str | None",
        # "username: str | None",
        # "seed_property: str | None",
    }

    # Collect all descriptions for each common parameter
    # parameter -> (method_name -> descriptions)
    param_descriptions: Dict[str, Dict[str, str]] = defaultdict(dict)

    methods = get_all_endpoint_methods(SessionV2Endpoints, "gds")

    for method_name, method in methods.items():
        docstring = inspect.getdoc(method)
        if not docstring:
            continue

        descriptions = extract_param_descriptions(docstring)
        param_types = inspect.signature(method).parameters

        for param in descriptions:
            typed_key = f"{param}: {param_types[param].annotation}" if param in param_types else param
            param_descriptions[typed_key][method_name] = descriptions[param]

    # print shared parameters
    # shared_params: dict[str, int] = {
    #     param: len(desc_per_method) for param, desc_per_method in param_descriptions.items() if len(desc_per_method) > 1
    # }
    # top_20_shared_params = dict(sorted(shared_params.items(), key=lambda item: item[1], reverse=True)[:20])
    # print(f"Top 20 shared parameters: {top_20_shared_params}")

    # Check consistency
    inconsistencies: list[dict[str, Any]] = []

    param_descriptions = {k: v for k, v in param_descriptions.items() if k in common_params}

    suggested_descriptions = load_parameter_descriptions()

    for param, desc_per_method in param_descriptions.items():
        if not desc_per_method:
            continue

        # Get all unique descriptions for this parameter
        all_descriptions: dict[str, list[str]] = defaultdict(list)
        for method, desc in desc_per_method.items():
            all_descriptions[desc] += [method]
        all_descriptions = dict(sorted(all_descriptions.items(), key=lambda x: len(x[1]), reverse=True))

        suggestions: list[str] | str | set[str] = suggested_descriptions.get(param.split(":")[0], set())
        if isinstance(suggestions, str):
            suggestions = {suggestions}

        if len(all_descriptions) > 1 and len(all_descriptions.keys() - suggestions) > 0:
            inconsistencies.append(
                {
                    "parameter": param,
                    "descriptions": all_descriptions,
                    "methods": list(desc_per_method.keys()),
                    "suggestion(s)": suggestions,
                }
            )

    # Report inconsistencies
    if inconsistencies:
        report = "\n\nInconsistent parameter descriptions found:\n"
        for issue in inconsistencies:
            report += f"\nParameter: {issue['parameter']}\n"
            report += f"Found in methods: {len(issue['methods'])}\n"
            report += "Different descriptions:\n"
            desc_options: dict[str, Any] = issue["descriptions"]  # type: ignore
            for desc, methods in desc_options.items():
                report += f" * {desc} ({len(methods)}x) - Example method: {methods[0]}\n"  # type: ignore
            report += f"Suggested description: {issue['suggestion(s)']}\n"
            report += "Copyable description regex: \n"
            desc_regex = "|".join([desc for desc in desc_options.keys()])
            report += f"({desc_regex})\n"

        pytest.fail(report)
