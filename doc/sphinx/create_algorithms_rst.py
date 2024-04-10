import json

from textwrap import dedent

with open("algorithms.json") as f, open("source/algorithms.rst", "w") as fw:
    functions = json.load(f)

    fw.write(dedent("""\
        ..
            DO NOT EDIT - File generated automatically

        Algorithms procedures
        ----------------------
        Listing of all algorithm procedures in the Neo4j Graph Data Science Python Client API.
        These all assume that an object of :class:`.GraphDataScience` is available as `gds`.

        """))
    
    for function in functions:
        name, sig, ret_type = function["function"]["name"], function["function"]["signature"], function["function"]["return_type"]
        fw.write(f".. py:function:: {name}({sig}) -> {ret_type}\n\n")

        if "description" in function:
          description = function["description"].strip()
          for desc in description.split("\n"):
              fw.write(f"    {desc}\n")

          fw.write("\n")

        if "deprecated" in function:
            version, message = function["deprecated"]["version"], function["deprecated"]["message"]
            fw.write(f".. deprecated:: {version}\n")
            fw.write(f"   {message}\n\n")
