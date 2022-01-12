# gdsclient

`gdsclient` is a Python wrapper API for operating and working with the [Neo4j Graph Data Science (GDS) library](https://github.com/neo4j/graph-data-science).
It enables users to write pure Python code to project graphs, run algorithms, and define and use machine learning pipelines in GDS.

The API is designed to mimic the GDS Cypher procedure API in Python code.
It abstracts the necessary operations of the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) to offer a simpler surface.

Please leave any feedback as issues on [the source repository](https://github.com/neo4j/gdsclient).
Happy coding!


## NOTE

This is a work in progress and some GDS features are known to be missing or not working properly (see [Known limitations](#known-limitations) below).
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
The calls return a list of dictionaries (with contents depending on the algorithm of course) as is also the case when using the Neo4j Python driver directly.

The other [algorithm execution modes](https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/) - stats, stream and write - are also supported via analogous calls.


#### Topological link prediction

The methods for doing [topological link prediction](https://neo4j.com/docs/graph-data-science/current/algorithms/linkprediction/) are a bit different.
Just like in the GDS procedure API they do not take a graph as an argument, but rather two node references as positional arguments.
And they simply return the similarity score of the prediction just made as a float - not a list of dictionaries.


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


### Machine learning models

In GDS, you can train machine learning models.
When doing this using the `gdsclient`, you can get a model object returned directly in the client.
The model object allows for convenient access to details about the model via Python methods.
It also offers the ability to directly compute predictions using the appropriate GDS procedure for that model.
This includes support for models trained using pipelines (for Link Prediction and Node Classification) as well as GraphSAGE models.


#### Pipelines

There's native support for [Link prediction pipelines](https://neo4j.com/docs/graph-data-science/current/algorithms/ml-models/linkprediction-pipelines/) and [Node classification pipelines](https://neo4j.com/docs/graph-data-science/current/algorithms/ml-models/nodeclassification-pipelines/).
Apart from the call to create a pipeline, the GDS native pipelines calls are represented by methods on pipeline Python objects.
Additionally to the standard GDS calls, there are several methods to query the pipeline for information about it.

Below is a minimal example for node classification (supposing we have a graph `G` with a property "myClass"):

```python
pipe = gds.alpha.ml.pipeline.nodeClassification.create("myPipe")
assert pipe.type() == "Node classification training pipeline"

pipe.addNodeProperty("degree", mutateProperty="rank")
pipe.selectFeatures("rank")
steps = pipe.feature_properties()
assert len(steps) == 1
assert steps[0]["feature"] == "rank"

trained_pipe = pipe.train(G, modelName="myModel", targetProperty="myClass", metrics=["ACCURACY"])
assert trained_pipe.metrics()["ACCURACY"]["test"] > 0

res = trained_pipe.predict_stream(G)
assert len(res) == G.node_count()
```

Link prediction works the same way, just with different method names for calls specific to that pipeline.
Please see the GDS documentation for more on the pipelines' procedure APIs.


#### GraphSAGE

Assuming we have a graph `G` with node property `x`, we can do the following:

```python
model = gds.beta.graphSage.train(G, modelName="myModel", featureProperties=["x"])
assert len(model.metrics()["epochLosses"]) == model.metrics()["ranEpochs"] 

res = model.predict_stream(G)
assert len(res) == G.node_count()
```

Note that with GraphSAGE we call the `train` method directly and supply all training configuration.


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


### Model catalog utils

All procedures from the [GDS Model catalog](https://neo4j.com/docs/graph-data-science/current/model-catalog/) are supported with `gdsclient`.
Some examples are (where `model` is a machine learning model object):

```python
res = gds.beta.model.list()
assert len(res) == 1  # Exactly one model is loaded

res = gds.beta.model.drop(model)
assert res[0]["modelInfo"]["modelName"] == model.name()
```

Further, there's a new call named `gds.model.get` (`gdsclient` only) which takes a model name as input and returns a model object if a model of that name exists in the user's model catalog.
The idea is to have a way of creating model objects for already loaded models, without having to create them again.


### Node matching without Cypher

When calling path finding or topological link prediction algorithms one has to provide specific nodes as input arguments.
When using the GDS procedure API directly to call such algorithms, typically Cypher `MATCH` statements are used in order to find valid representations of input nodes of interest, see eg. [this example in the GDS docs](https://neo4j.com/docs/graph-data-science/current/algorithms/dijkstra-source-target/#algorithms-dijkstra-source-target-examples-stream).
To simplify this, `gdsclient` provides a utility function, `gds.find_node_id`, for letting one find nodes without using Cypher.

Below is an example of how this can be done (supposing `G` is a projected `Graph` with `City` nodes having `name` properties):

```python
# gds.find_node_id takes a list of labels and a dictionary of
# property key-value pairs
source_id = gds.find_node_id(["City"], {"name": "New York"})
target_id = gds.find_node_id(["City"], {"name": "Philadelphia"})

res = gds.shortestPath.dijkstra.stream(G, sourceNode=source_id, targetNode=target_id)
assert res[0]["totalCost"] == 100
```

The nodes found by `gds.find_node_id` are those that have all labels specified and fully match all property key-value pairs given.
Note that exactly one node per method call must be matched.

For more advanced filtering we recommend users do matching via Cypher's `MATCH`.


## Known limitations

Operations known to not yet work with `gdsclient`:

* Some utility functions


## License

`gdsclient` is licensed under the Apache Software License version 2.0.
All content is copyright © Neo4j Sweden AB.


## Acknowledgements

This work has been inspired by the great work done in the following libraries:

* [pygds](https://github.com/stellasia/pygds) by stellasia
* [gds-python](https://github.com/moxious/gds-python) by moxious
