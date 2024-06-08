import json
from textwrap import dedent

INCLUDED_ALGORITHMS = {
    "Article Rank",
    "Betweenness Centrality",
    # "CELF",
    # "Closeness Centrality",
    "Degree Centrality",
    "Eigenvector Centrality",
    "PageRank",
    # "Harmonic Centrality",
    # "HITS",
    "Conductance metric",
    # "K-Core Decomposition",
    # "K-1 Coloring",
    # "K-Means Clustering",
    # "Label Propagation",
    # "Leiden",
    "Local Clustering Coefficient",
    # "Louvain",
    "Modularity metric",
    # "Modularity Optimization",
    # "Strongly Connected Components",
    "Triangle Count",
    # "Weakly Connected Components",
    # "Approximate Maximum k-cut",
    # "Speaker-Listener Label Propagation",
    "Node Similarity",
    # "Filtered Node Similarity",
    # "K-Nearest Neighbors",
    # "Filtered K-Nearest Neighbors",
    "Delta-Stepping Single-Source Shortest Path",
    # "Dijkstra Source-Target Shortest Path",
    # "Dijkstra Single-Source Shortest Path",
    # "A* Shortest Path",
    # "Yen's Shortest Path algorithm",
    # "Minimum Weight Spanning Tree",
    # "Minimum Directed Steiner Tree",
    # "Random Walk",
    "Breadth First Search",
    "Depth First Search",
    # "Bellman-Ford Single-Source Shortest Path",
    # "Longest Path for DAG",
    # "All Pairs Shortest Path",
    # "Topological Sort",
    # "Longest Path for DAG",
    "Fast Random Projection",
    "GraphSAGE",
    "Node2Vec",
    "HashGNN",
}

def write_param(param, optional):
    name, description, default = conf["name"], conf["description"], conf["default"]
    default_placeholder = f' ({conf["default_placeholder"]})' if "default_placeholder" in conf else ""
    description = description.replace("\n", "\n                ")

    if optional:
        return f"        * **{name}** - *(Optional)* {description} *Default*: {default}{default_placeholder}."
    else:
        return f"        * **{name}** - {description}"

configs = []
with open("algorithms-conf.json") as f:
    j = json.load(f)
    configs = j["algorithms"]
    modes = j["modes"]

procedures = {config["procedure"]: config for config in configs if config["name"] in INCLUDED_ALGORITHMS}

with open("algorithms.json") as f, open("source/algorithms.rst", "w") as fw:
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

    for function in functions:
        name, sig, ret_type = (
            function["function"]["name"],
            function["function"]["signature"],
            function["function"]["return_type"],
        )

        conf_string = []
        proc_name = name.rsplit(".", maxsplit=1)
        if proc_name[0] in procedures and len(procedures[proc_name[0]]["config"]) and proc_name[1] == "stream":
            required = [conf for conf in procedures[proc_name[0]]["config"] if not conf["optional"]]
            optional = [conf for conf in procedures[proc_name[0]]["config"] if conf["optional"]]
            optional = [conf for conf in modes["stream"]["config"] if conf["optional"]] + optional

            for conf in required:
                conf_string.append(conf["name"])

            for conf in optional:
                conf_name, conf_type, conf_default = conf["name"], conf["type"], conf["default"]
                # if conf_type == "Float":
                #     try:
                #         conf_default = float(conf_default)
                #     except:
                #         print(f"{conf_default} not a float")
                # elif conf_type == "Integer":
                #     try:
                #         conf_default = int(conf_default)
                #     except:
                #         print(f"{conf_default} not an int")
                
                if conf_default == "null":
                    conf_default = None
                
                conf_string.append(f"{conf_name}={conf_default}")
        
        if conf_string:
            sig_fixed = sig.replace("**config: Any", f"*, {', '.join(conf_string)}")
        else:
            sig_fixed = sig

        fw.write(f".. py:function:: {name}({sig_fixed}) -> {ret_type}\n\n")

        if "description" in function:
            description = function["description"].strip()
            for desc in description.split("\n"):
                fw.write(f"    {desc}\n")

            fw.write("\n")

        if "deprecated" in function:
            version, message = function["deprecated"]["version"], function["deprecated"]["message"]
            fw.write(f".. deprecated:: {version}\n")
            fw.write(f"   {message}\n\n")

        fw.write("    |\n\n")
        fw.write("    **Parameters:**\n\n")

        for param in sig.split(","):
            param_name, param_type = param.split(":")
            param_name = param_name.strip()
            param_type = param_type.strip()

            if param_name != "**config":
                fw.write(f"        * **{param_name}** - {param_type}\n\n")
            else:
                proc_name = name.rsplit(".", maxsplit=1)
                if proc_name[0] in procedures and len(procedures[proc_name[0]]["config"]) and proc_name[1] == "stream":
                    required = [conf for conf in procedures[proc_name[0]]["config"] if not conf["optional"]]
                    optional = [conf for conf in procedures[proc_name[0]]["config"] if conf["optional"]]

                    optional = [conf for conf in modes["stream"]["config"] if conf["optional"]] + optional

                    for conf in required:
                        fw.write(write_param(conf, False) + "\n\n")

                    for conf in optional:
                        fw.write(write_param(conf, True) + "\n\n")
                        
                    fw.write("\n\n")
