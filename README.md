# Neo4j Graph Data Science Client

[![Latest version](https://img.shields.io/pypi/v/graphdatascience)](https://pypi.org/project/graphdatascience/)
[![PyPI downloads month](https://img.shields.io/pypi/dm/graphdatascience)](https://pypi.org/project/graphdatascience/)
![Python versions](https://img.shields.io/pypi/pyversions/graphdatascience)
[![Documentation](https://img.shields.io/badge/Documentation-latest-blue)](https://neo4j.com/docs/graph-data-science-client/current/)
[![Discord](https://img.shields.io/discord/787399249741479977?label=Chat&logo=discord)](https://discord.gg/neo4j)
[![Community forum](https://img.shields.io/website?down_color=lightgrey&down_message=offline&label=Forums&logo=discourse&up_color=green&up_message=online&url=https%3A%2F%2Fcommunity.neo4j.com%2F)](https://community.neo4j.com)
[![License](https://img.shields.io/pypi/l/graphdatascience)](https://www.apache.org/licenses/LICENSE-2.0)

`graphdatascience` is a Python client for operating and working with the [Neo4j Graph Data Science (GDS) library](https://github.com/neo4j/graph-data-science).
It enables users to write pure Python code to project graphs, run algorithms, as well as define and use machine learning pipelines in GDS.

The API is designed to mimic the GDS Cypher procedure API in Python code.
It abstracts the necessary operations of the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) to offer a simpler surface.

`graphdatascience` is only guaranteed to work with GDS versions 2.0+.

Please leave any feedback as issues on [the source repository](https://github.com/neo4j/graph-data-science-client).
Happy coding!


## Installation

To install the latest deployed version of `graphdatascience`, simply run:

```bash
pip install graphdatascience
```


## Usage

What follows is a high level description of some of the operations supported by `graphdatascience`.
For extensive documentation of all capabilities, please refer to the [GDS Python Client Manual](https://neo4j.com/docs/graph-data-science-client/current/).

Extensive end-to-end examples in Jupyter ready-to-run notebooks can be found in the [`examples` source directory](https://github.com/neo4j/graph-data-science-client/tree/main/examples):

* [Product recommendations with kNN based on FastRP embeddings](https://github.com/neo4j/graph-data-science-client/tree/main/examples/fastrp-and-knn.ipynb)
* [Exporting from GDS and running GCN with PyG](https://github.com/neo4j/graph-data-science-client/tree/main/examples/import-sample-export-gnn.ipynb)
* [Load data to a projected graph via graph construction](https://github.com/neo4j/graph-data-science-client/tree/main/examples/load-data-via-graph-construction.ipynb)


### Imports and setup

The library wraps the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) with a `GraphDataScience` object through which most calls to GDS will be made.

```python
from graphdatascience import GraphDataScience

# Use Neo4j URI and credentials according to your setup
gds = GraphDataScience("bolt://localhost:7687", auth=None)
```

There's also a method `GraphDataScience.from_neo4j_driver` for instantiating the `gds` object directly from a Neo4j driver object.

If we don't want to use the default database of our DBMS, we can specify which one to use:

```python
gds.set_database("my-db")
```


#### AuraDS

If you are connecting the client to an [AuraDS instance](https://neo4j.com/cloud/graph-data-science/), you can get recommended non-default configuration settings of the Python Driver applied automatically.
To achieve this, set the constructor argument `aura_ds=True`:

```python
from graphdatascience import GraphDataScience

# Configures the driver with AuraDS-recommended settings
gds = GraphDataScience("neo4j+s://my-aura-ds.databases.neo4j.io:7687", auth=("neo4j", "my-password"), aura_ds=True)
```


### Projecting a graph from Neo4j

Supposing that we have some graph data in our Neo4j database, we can [project the graph into memory](https://neo4j.com/docs/graph-data-science/current/graph-project/).

```python
# Optionally we can estimate memory of the operation first
res = gds.graph.project.estimate("*", "*")
assert res["bytesMax"] < 1e12

G, res = gds.graph.project("graph", "*", "*")
assert res["projectMillis"] >= 0
```

The `G` that is returned here is a `Graph` which on the client side represents the projection on the server side.

The analogous calls `gds.graph.project.cypher{,.estimate}` for [Cypher based projection](https://neo4j.com/docs/graph-data-science/current/graph-project-cypher/) are also supported.


### Constructing a graph from data frames

We can also construct a GDS graph from client side pandas `DataFrame`s.
To do this we provide the `gds.alpha.graph.construct` method with node data frames (see schema [here](https://neo4j.com/docs/graph-data-science/current/graph-project-apache-arrow/#arrow-send-nodes)) and relationship data frames (see schema [here](https://neo4j.com/docs/graph-data-science/current/graph-project-apache-arrow/#arrow-send-relationships)).

```python
nodes = pandas.DataFrame(
    {
        "nodeId": [0, 1, 2, 3],
        "labels":  ["A", "B", "C", "A"],
        "prop1": [42, 1337, 8, 0],
        "otherProperty": [0.1, 0.2, 0.3, 0.4]
    }
)

relationships = pandas.DataFrame(
    {
        "sourceNodeId": [0, 1, 2, 3],
        "targetNodeId": [1, 2, 3, 0],
        "relationshipType": ["REL", "REL", "REL", "REL"],
        "weight": [0.0, 0.0, 0.1, 42.0]
    }
)

G = gds.alpha.graph.construct(
    "my-graph",      # Graph name
    nodes,           # One or more dataframes containing node data
    relationships    # One or more dataframes containing relationship data
)
```

If your server uses GDS Enterprise edition and you have [enabled its Arrow Apache server](https://neo4j.com/docs/graph-data-science/current/installation/installation-apache-arrow/), the construction will be a lot faster.
In this case you must also make sure that you have explicitly specified which database to use via `GraphDataScience.set_database`.


### Running algorithms

We can take a projected graph, represented to us by a `Graph` object named `G`, and run [algorithms](https://neo4j.com/docs/graph-data-science/current/algorithms/) on it.

```python
# Optionally we can estimate memory of the operation first (if the algo supports it)
res = gds.pageRank.mutate.estimate(G, tolerance=0.5, mutateProperty="pagerank")
assert res["bytesMax"] < 1e12

res = gds.pageRank.mutate(G, tolerance=0.5, mutateProperty="pagerank")
assert res["nodePropertiesWritten"] == G.node_count()
```

These calls take one positional argument and a number of keyword arguments depending on the algorithm.
The first (positional) argument is a `Graph`, and the keyword arguments map directly to the algorithm's [configuration map](https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/#algorithms-syntax-configuration-parameters).

The other [algorithm execution modes](https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/) - stats, stream and write - are also supported via analogous calls.
The stream mode call returns a pandas DataFrame (with contents depending on the algorithm of course).
The mutate, stats and write mode calls however return a pandas Series with metadata about the algorithm execution.


#### Topological link prediction

The methods for doing [topological link prediction](https://neo4j.com/docs/graph-data-science/current/algorithms/linkprediction/) are a bit different.
Just like in the GDS procedure API they do not take a graph as an argument, but rather two node references as positional arguments.
And they simply return the similarity score of the prediction just made as a float - not any kind of pandas data structure.


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
When doing this using the `graphdatascience`, you can get a model object returned directly in the client.
The model object allows for convenient access to details about the model via Python methods.
It also offers the ability to directly compute predictions using the appropriate GDS procedure for that model.
This includes support for models trained using pipelines (for Link Prediction and Node Classification) as well as GraphSAGE models.


#### Pipelines

There's native support for [Link prediction pipelines](https://neo4j.com/docs/graph-data-science/current/algorithms/ml-models/linkprediction-pipelines/), [Node classification pipelines](https://neo4j.com/docs/graph-data-science/current/algorithms/ml-models/nodeclassification-pipelines/), and [Node regression pipeline](https://neo4j.com/docs/graph-data-science/2.1-preview/machine-learning/node-property-prediction/noderegression-pipelines/).
Apart from the call to create a pipeline, the GDS native pipelines calls are represented by methods on pipeline Python objects.
Additionally to the standard GDS calls, there are several methods to query the pipeline for information about it.

Below is a minimal example for node classification (supposing we have a graph `G` with a property "myClass"):

```python
pipe, _ = gds.beta.pipeline.nodeClassification.create("myPipe")
assert pipe.type() == "Node classification training pipeline"

pipe.addNodeProperty("degree", mutateProperty="rank")
pipe.selectFeatures("rank")
steps = pipe.feature_properties()
assert len(steps) == 1
assert steps[0]["feature"] == "rank"

pipe.addLogisticRegression(penalty=(0.1, 2))

model, res = pipe.train(G, modelName="myModel", targetProperty="myClass", metrics=["ACCURACY"])
assert model.metrics()["ACCURACY"]["test"] > 0
assert res["trainMillis"] >= 0

res = model.predict_stream(G)
assert len(res) == G.node_count()
```

Link prediction and Node regression works the same way, just with different method names for calls specific to that pipeline.
Please see the GDS documentation for more on the pipelines' procedure APIs.


#### GraphSAGE

Assuming we have a graph `G` with node property `x`, we can do the following:

```python
model, res = gds.beta.graphSage.train(G, modelName="myModel", featureProperties=["x"])
assert len(model.metrics()["epochLosses"]) == model.metrics()["ranEpochs"] 
assert res["trainMillis"] >= 0

res = model.predict_stream(G)
assert len(res) == G.node_count()
```

Note that with GraphSAGE we call the `train` method directly and supply all training configuration.


### Graph catalog utils

All procedures from the [GDS Graph catalog](https://neo4j.com/docs/graph-data-science/current/management-ops/graph-catalog-ops/) are supported with `graphdatascience`.
Some examples are (where `G` is a `Graph`):

```python
res = gds.graph.list()
assert len(res) == 1  # Exactly one graph is projected

res = gds.graph.streamNodeProperties(G, "rank")
assert len(res) == G.node_count()
```

Further, there's a call named `gds.graph.get` (`graphdatascience` only).
It takes a graph name as input and returns a `Graph` object, if a graph projection of that name exists in the user's graph catalog.
The idea is to have a way of creating `Graph`s for already projected graphs, without having to do a new projection.


### Pipeline catalog utils

All procedures from the [GDS Pipeline catalog](https://neo4j.com/docs/graph-data-science/current/pipeline-catalog/) are supported with `graphdatascience`.
Some examples are (where `pipe` is a machine learning training pipeline object):

```python
res = gds.beta.pipeline.list()
assert len(res) == 1  # Exactly one pipeline is in the catalog

res = gds.beta.pipeline.drop(pipe)
assert res["pipelineName"] == pipe.name()
```

Further, there's a call named `gds.pipeline.get` (`graphdatascience` only).
It takes a pipeline name as input and returns a training pipeline object, if a pipeline of that name exists in the user's pipeline catalog.
The idea is to have a way of creating pipeline objects for already existing pipelines, without having to create them again.


### Model catalog utils

All procedures from the [GDS Model catalog](https://neo4j.com/docs/graph-data-science/current/model-catalog/) are supported with `graphdatascience`.
Some examples are (where `model` is a machine learning model object):

```python
res = gds.beta.model.list()
assert len(res) == 1  # Exactly one model is loaded

res = gds.beta.model.drop(model)
assert res["modelInfo"]["modelName"] == model.name()
```

Further, there's a call named `gds.model.get` (`graphdatascience` only).
It takes a model name as input and returns a model object, if a model of that name exists in the user's model catalog.
The idea is to have a way of creating model objects for already loaded models, without having to create them again.


### Node matching without Cypher

When calling path finding or topological link prediction algorithms one has to provide specific nodes as input arguments.
When using the GDS procedure API directly to call such algorithms, typically Cypher `MATCH` statements are used in order to find valid representations of input nodes of interest, see eg. [this example in the GDS docs](https://neo4j.com/docs/graph-data-science/current/algorithms/dijkstra-source-target/#algorithms-dijkstra-source-target-examples-stream).
To simplify this, `graphdatascience` provides a utility function, `gds.find_node_id`, for letting one find nodes without using Cypher.

Below is an example of how this can be done (supposing `G` is a projected `Graph` with `City` nodes having `name` properties):

```python
# gds.find_node_id takes a list of labels and a dictionary of
# property key-value pairs
source_id = gds.find_node_id(["City"], {"name": "New York"})
target_id = gds.find_node_id(["City"], {"name": "Philadelphia"})

res = gds.shortestPath.dijkstra.stream(G, sourceNode=source_id, targetNode=target_id)
assert res["totalCost"][0] == 100
```

The nodes found by `gds.find_node_id` are those that have all labels specified and fully match all property key-value pairs given.
Note that exactly one node per method call must be matched.

For more advanced filtering we recommend users do matching via Cypher's `MATCH`.


## Known limitations

Operations known to not yet work with `graphdatascience`:

* [Numeric utility functions](https://neo4j.com/docs/graph-data-science/current/management-ops/utility-functions/#utility-functions-numeric) (will never be supported)
* [Cypher on GDS](https://neo4j.com/docs/graph-data-science/current/management-ops/create-cypher-db/) (might be supported in the future)


## License

`graphdatascience` is licensed under the Apache Software License version 2.0.
All content is copyright © Neo4j Sweden AB.


## Acknowledgements

This work has been inspired by the great work done in the following libraries:

* [pygds](https://github.com/stellasia/pygds) by stellasia
* [gds-python](https://github.com/moxious/gds-python) by moxious
