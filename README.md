# gdsclient

`gdsclient` is a Python wrapper API for operating and working with the [Neo4j Graph Data Science (GDS) library](https://github.com/neo4j/graph-data-science).
It enables users to write pure Python code to project graphs, run algorithms, and define and use machine learning pipelines in GDS.

The API is designed to mimic the GDS Cypher procedure API in Python code.
It abstracts the necessary operations of the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) to offer a simpler surface.

Please leave any feedback as issues on [the source repository](https://github.com/neo4j/gdsclient).
Happy coding!


## NOTE

This is a work in progress and several GDS features are known to be missing or not working properly (see [Known limitations](#known-limitations) below).
Further, this library targets GDS versions 2.0+ (not yet released) and as such may not work with older versions.


## Installation

To install the latest deployed version of `gdsclient`, simply run:

```bash
pip install gdsclient
```


## Usage

What follows is a high level description of some of the operations supported by `gdsclient`.
For extensive documentation of all operations supported by GDS, please refer to the [GDS Manual](https://neo4j.com/docs/graph-data-science/current/).

Extensive end-to-end examples in Jupyter ready-to-run notebooks can be found in the [`examples` source directory](https://github.com/neo4j/gdsclient/tree/main/examples):

* [Computing similarities with kNN based on FastRP embeddings](https://github.com/neo4j/gdsclient/tree/main/examples/fastrp-and-knn.ipynb)


### Imports and setup

The library wraps the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) with a `GraphDataScience` object through which most calls to GDS will be made.

```python
from neo4j import GraphDatabase
from gdsclient import Neo4jQueryRunner, GraphDataScience

# Replace Neo4j Python driver settings according to your setup
URI = "bolt://localhost:7687"
driver = GraphDatabase.driver(URI)
gds = GraphDataScience(Neo4jQueryRunner(driver))
gds.set_database("my-db")  # (Optional) Use a specific Neo4j database
```


### Projecting a graph

Supposing that we have some graph data in our Neo4j database, we can [project the graph into memory](https://neo4j.com/docs/graph-data-science/current/graph-create/).

```python
# Optionally we can estimate memory of the operation first
res = gds.graph.project.estimate("*", "*")
assert res[0]["requiredMemory"] < 1e12

G = gds.graph.project("graph", "*", "*")
```

The `G` that is returned here is a `Graph` which on the client side represents the projection on the server side.

The analogous calls `gds.graph.project.cypher{,.estimate}` for [Cypher based projection](https://neo4j.com/docs/graph-data-science/current/graph-create-cypher/) are also supported.


### Running algorithms

We can take a projected graph, represented to us by a `Graph` object named `G`, and run [algorithms](https://neo4j.com/docs/graph-data-science/current/algorithms/) on it.

```python
# Optionally we can estimate memory of the operation first (if the algo supports it)
res = gds.pageRank.mutate.estimate(G, tolerance=0.5, writeProperty="pagerank")
assert res[0]["requiredMemory"] < 1e12

res = gds.pageRank.mutate(G, tolerance=0.5, writeProperty="pagerank")
assert res[0]["nodePropertiesWritten"] == G.node_count()
```

These calls take one positional argument and a number of keyword arguments depending on the algorithm.
The first (positional) argument is a `Graph`, and the keyword arguments map directly to the algorithm's [configuration map](https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/#algorithms-syntax-configuration-parameters).

The other [algorithm execution modes](https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/) - stats, stream and write - are also supported via analogous calls.

Though most algorithms are supported this way, not all are yet.
Please see [Known limitations](#known-limitations) below for more on this.


### The Graph object

In this library, graphs projected onto server-side memory are represented by `Graph` objects.
There are convenience methods on the `Graph` object that let us extract information about our projected graph.
Some examples are (where `G` is a `Graph`):

```python
# Get the graph's node count
n = G.node_count()

# Get a list of all relationship properties present on
# relationships of the type "myRelType"
rel_props = G.relationship_properties("myRelType")

# Drop the projection represented by G
G.drop()
```


### Graph catalog utils

All procedures from the [GDS Graph catalog](https://neo4j.com/docs/graph-data-science/current/management-ops/graph-catalog-ops/) are supported with `gdsclient`.
Some examples are (where `G` is a `Graph`):

```python
res = gds.graph.list()
assert len(res) == 1  # Exactly one graph is projected

res = gds.graph.streamNodeProperties(G, "rank")
assert len(res) == G.node_count()
```

Further, there's a new call named `gds.graph.get` (`gdsclient` only) which takes a name as input and returns a `Graph` object if a graph projection of that name exists in the user's graph catalog.
The idea is to have a way of creating `Graph`s for already projected graphs, without having to do a new projection.

## Known limitations

Several operations are known to not yet work with `gdsclient`:

* Path finding algorithms
* Topological link prediction
* Supervised machine learning (GraphSAGE, Link prediction, Node classification)
* Progress logging and system monitoring
* Some utility functions


## License

`gdsclient` is licensed under the Apache Software License version 2.0.
All content is copyright © Neo4j Sweden AB.


## Acknowledgements

This work has been inspired by the great work done in the following libraries:

* [pygds](https://github.com/stellasia/pygds) by stellasia
* [gds-python](https://github.com/moxious/gds-python) by moxious
