import json
from pathlib import Path
from textwrap import dedent

root_dir = Path(__file__).parent


with open(root_dir / "algorithms.json") as f, open(root_dir / "source/algorithms.rst", "w") as fw:
    functions = json.load(f)

    fw.write(
        dedent(
            """\
        ..
            DO NOT EDIT - File generated automatically

        Algorithms procedures
        ----------------------
        Listing of all algorithm procedures in the Neo4j Graph Data Science Python Client API.
        These all assume that an object of :class:`.GraphDataScience` is available as `gds`.

        """
        )
    )

    algo_sections: dict[str, list[str]] = {}

    for function in functions:
        name, sig, ret_type = (
            function["function"]["name"],
            function["function"]["signature"],
            function["function"]["return_type"],
        )
        algo_lines = []

        algo_lines.append(f".. py:function:: {name}({sig}) -> {ret_type}\n\n")

        if "description" in function:
            description = function["description"].strip()
            for desc in description.split("\n"):
                algo_lines.append(f"    {desc}\n")

            algo_lines.append("\n")

        if "deprecated" in function:
            version, message = function["deprecated"]["version"], function["deprecated"]["message"]
            algo_lines.append(f".. deprecated:: {version}\n")
            algo_lines.append(f"   {message}\n\n")

        algo_sections[name] = algo_lines

    # sort by name
    sorted_section = sorted(algo_sections.items(), key=lambda x: x[0])

    for name, section in sorted_section:
        fw.writelines(section)
