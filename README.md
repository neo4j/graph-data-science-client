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
Additionally, the client-specific graph, model, and pipeline objects offer convenient functions that heavily reduce the need to use Cypher to access and operate these GDS resources.

`graphdatascience` is only guaranteed to work with GDS versions 2.0+.

Please leave any feedback as issues on [the source repository](https://github.com/neo4j/graph-data-science-client).
Happy coding!


## Installation

To install the latest deployed version of `graphdatascience`, simply run:

```bash
pip install graphdatascience
```


## Getting started

To use the GDS Python Client, we need to instantiate a GraphDataScience object.
Then, we can project graphs, create pipelines, train models, and run algorithms.

```python
from graphdatascience import GraphDataScience

# Configure the driver with AuraDS-recommended settings
gds = GraphDataScience("neo4j+s://my-aura-ds.databases.neo4j.io:7687", auth=("neo4j", "my-password"), aura_ds=True)

# Import the Cora common dataset to GDS
G = gds.graph.load_cora()
assert G.node_count() == 2708

# Run PageRank in mutate mode on G
pagerank_result = gds.pageRank.mutate(G, tolerance=0.5, mutateProperty="pagerank")
assert pagerank_result["nodePropertiesWritten"] == G.node_count()

# Create a Node Classification pipeline
pipeline = gds.nc_pipe("myPipe")
assert pipeline.type() == "Node classification training pipeline"

# Add a Degree Centrality feature to the pipeline
pipeline.addNodeProperty("degree", mutateProperty="rank")
pipeline.selectFeatures("rank")
features = pipeline.feature_properties()
assert len(features) == 1
assert features[0]["feature"] == "rank"

# Add a training method
pipeline.addLogisticRegression(penalty=(0.1, 2))

# Train a model on G
model, train_result = pipeline.train(G, modelName="myModel", targetProperty="myClass", metrics=["ACCURACY"])
assert model.metrics()["ACCURACY"]["test"] > 0
assert train_result["trainMillis"] >= 0

# Compute predictions in stream mode
predictions = model.predict_stream(G)
assert len(predictions) == G.node_count()
```

The example here assumes using an AuraDS instance.
For additional examples and extensive documentation of all capabilities, please refer to the [GDS Python Client Manual](https://neo4j.com/docs/graph-data-science-client/current/).

Full end-to-end examples in Jupyter ready-to-run notebooks can be found in the [`examples` source directory](https://github.com/neo4j/graph-data-science-client/tree/main/examples):

* [Product recommendations with kNN based on FastRP embeddings](examples/fastrp-and-knn.ipynb)
* [Exporting from GDS and running GCN with PyG](https://github.com/neo4j/graph-data-science-client/tree/main/examples/import-sample-export-gnn.ipynb)
* [Load data to a projected graph via graph construction](https://github.com/neo4j/graph-data-science-client/tree/main/examples/load-data-via-graph-construction.ipynb)


[//]: # (### Imports and setup)

[//]: # ()
[//]: # (The library wraps the [Neo4j Python driver]&#40;https://neo4j.com/docs/python-manual/current/&#41; with a `GraphDataScience` object through which most calls to GDS will be made.)

[//]: # ()
[//]: # (```python)

[//]: # (from graphdatascience import GraphDataScience)

[//]: # ()
[//]: # (# Use Neo4j URI and credentials according to your setup)

[//]: # (gds = GraphDataScience&#40;"bolt://localhost:7687", auth=None&#41;)

[//]: # (```)

[//]: # ()
[//]: # ()
[//]: # (#### AuraDS)

[//]: # ()
[//]: # (If you are connecting the client to an [AuraDS instance]&#40;https://neo4j.com/cloud/graph-data-science/&#41;, you can get recommended non-default configuration settings of the Python Driver applied automatically.)

[//]: # (To achieve this, set the constructor argument `aura_ds=True`:)

[//]: # ()
[//]: # (```python)

[//]: # (from graphdatascience import GraphDataScience)

[//]: # ()
[//]: # (# Configures the driver with AuraDS-recommended settings)

[//]: # (gds = GraphDataScience&#40;"neo4j+s://my-aura-ds.databases.neo4j.io:7687", auth=&#40;"neo4j", "my-password"&#41;, aura_ds=True&#41;)

[//]: # (```)


[//]: # (### Projecting a graph from Neo4j)

[//]: # ()
[//]: # (Supposing that we have some graph data in our Neo4j database, we can [project the graph into memory]&#40;https://neo4j.com/docs/graph-data-science/current/graph-project/&#41;.)

[//]: # ()
[//]: # (```python)

[//]: # (# Optionally we can estimate memory of the operation first)

[//]: # (res = gds.graph.project.estimate&#40;"*", "*"&#41;)

[//]: # (assert res["bytesMax"] < 1e12)

[//]: # ()
[//]: # (G, res = gds.graph.project&#40;"graph", "*", "*"&#41;)

[//]: # (assert res["projectMillis"] >= 0)

[//]: # (```)

[//]: # ()
[//]: # (The `G` that is returned here is a `Graph` which on the client side represents the projection on the server side.)

[//]: # ()
[//]: # (The analogous calls `gds.graph.project.cypher{,.estimate}` for [Cypher based projection]&#40;https://neo4j.com/docs/graph-data-science/current/graph-project-cypher/&#41; are also supported.)


[//]: # (### Constructing a graph from data frames)

[//]: # ()
[//]: # (We can also construct a GDS graph from client side pandas `DataFrame`s.)

[//]: # (To do this we provide the `gds.alpha.graph.construct` method with node data frames &#40;see schema [here]&#40;https://neo4j.com/docs/graph-data-science/current/graph-project-apache-arrow/#arrow-send-nodes&#41;&#41; and relationship data frames &#40;see schema [here]&#40;https://neo4j.com/docs/graph-data-science/current/graph-project-apache-arrow/#arrow-send-relationships&#41;&#41;.)

[//]: # ()
[//]: # (```python)

[//]: # (nodes = pandas.DataFrame&#40;)

[//]: # (    {)

[//]: # (        "nodeId": [0, 1, 2, 3],)

[//]: # (        "labels":  ["A", "B", "C", "A"],)

[//]: # (        "prop1": [42, 1337, 8, 0],)

[//]: # (        "otherProperty": [0.1, 0.2, 0.3, 0.4])

[//]: # (    })

[//]: # (&#41;)

[//]: # ()
[//]: # (relationships = pandas.DataFrame&#40;)

[//]: # (    {)

[//]: # (        "sourceNodeId": [0, 1, 2, 3],)

[//]: # (        "targetNodeId": [1, 2, 3, 0],)

[//]: # (        "relationshipType": ["REL", "REL", "REL", "REL"],)

[//]: # (        "weight": [0.0, 0.0, 0.1, 42.0])

[//]: # (    })

[//]: # (&#41;)

[//]: # ()
[//]: # (G = gds.alpha.graph.construct&#40;)

[//]: # (    "my-graph",      # Graph name)

[//]: # (    nodes,           # One or more dataframes containing node data)

[//]: # (    relationships    # One or more dataframes containing relationship data)

[//]: # (&#41;)

[//]: # (```)

[//]: # ()
[//]: # (If your server uses GDS Enterprise edition and you have [enabled its Arrow Apache server]&#40;https://neo4j.com/docs/graph-data-science/current/installation/installation-apache-arrow/&#41;, the construction will be a lot faster.)

[//]: # (In this case you must also make sure that you have explicitly specified which database to use via `GraphDataScience.set_database`.)

[//]: # ()

[//]: # (### Running algorithms)

[//]: # ()
[//]: # (We can take a projected graph, represented to us by a `Graph` object named `G`, and run [algorithms]&#40;https://neo4j.com/docs/graph-data-science/current/algorithms/&#41; on it.)

[//]: # ()
[//]: # (```python)

[//]: # (# Optionally we can estimate memory of the operation first &#40;if the algo supports it&#41;)

[//]: # (res = gds.pageRank.mutate.estimate&#40;G, tolerance=0.5, mutateProperty="pagerank"&#41;)

[//]: # (assert res["bytesMax"] < 1e12)

[//]: # ()
[//]: # (res = gds.pageRank.mutate&#40;G, tolerance=0.5, mutateProperty="pagerank"&#41;)

[//]: # (assert res["nodePropertiesWritten"] == G.node_count&#40;&#41;)

[//]: # (```)

[//]: # ()
[//]: # (These calls take one positional argument and a number of keyword arguments depending on the algorithm.)

[//]: # (The first &#40;positional&#41; argument is a `Graph`, and the keyword arguments map directly to the algorithm's [configuration map]&#40;https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/#algorithms-syntax-configuration-parameters&#41;.)

[//]: # ()
[//]: # (The other [algorithm execution modes]&#40;https://neo4j.com/docs/graph-data-science/current/common-usage/running-algos/&#41; - stats, stream and write - are also supported via analogous calls.)

[//]: # (The stream mode call returns a pandas DataFrame &#40;with contents depending on the algorithm of course&#41;.)

[//]: # (The mutate, stats and write mode calls however return a pandas Series with metadata about the algorithm execution.)


[//]: # (#### Topological link prediction)

[//]: # ()
[//]: # (The methods for doing [topological link prediction]&#40;https://neo4j.com/docs/graph-data-science/current/algorithms/linkprediction/&#41; are a bit different.)

[//]: # (Just like in the GDS procedure API they do not take a graph as an argument, but rather two node references as positional arguments.)

[//]: # (And they simply return the similarity score of the prediction just made as a float - not any kind of pandas data structure.)


[//]: # (### The Graph object)

[//]: # ()
[//]: # (In this library, graphs projected onto server-side memory are represented by `Graph` objects.)

[//]: # (There are convenience methods on the `Graph` object that let us extract information about our projected graph.)

[//]: # (Some examples are &#40;where `G` is a `Graph`&#41;:)

[//]: # ()
[//]: # (```python)

[//]: # (# Get the graph's node count)

[//]: # (n = G.node_count&#40;&#41;)

[//]: # ()
[//]: # (# Get a list of all relationship properties present on)

[//]: # (# relationships of the type "myRelType")

[//]: # (rel_props = G.relationship_properties&#40;"myRelType"&#41;)

[//]: # ()
[//]: # (# Drop the projection represented by G)

[//]: # (G.drop&#40;&#41;)

[//]: # (```)


[//]: # (### Machine learning models)

[//]: # ()
[//]: # (In GDS, you can train machine learning models.)

[//]: # (When doing this using the `graphdatascience`, you can get a model object returned directly in the client.)

[//]: # (The model object allows for convenient access to details about the model via Python methods.)

[//]: # (It also offers the ability to directly compute predictions using the appropriate GDS procedure for that model.)

[//]: # (This includes support for models trained using pipelines &#40;for Link Prediction and Node Classification&#41; as well as GraphSAGE models.)

[//]: # ()
[//]: # ()
[//]: # (#### Pipelines)

[//]: # ()
[//]: # (There's native support for [Link prediction pipelines]&#40;https://neo4j.com/docs/graph-data-science/current/algorithms/ml-models/linkprediction-pipelines/&#41;, [Node classification pipelines]&#40;https://neo4j.com/docs/graph-data-science/current/algorithms/ml-models/nodeclassification-pipelines/&#41;, and [Node regression pipeline]&#40;https://neo4j.com/docs/graph-data-science/2.1-preview/machine-learning/node-property-prediction/noderegression-pipelines/&#41;.)

[//]: # (Apart from the call to create a pipeline, the GDS native pipelines calls are represented by methods on pipeline Python objects.)

[//]: # (Additionally to the standard GDS calls, there are several methods to query the pipeline for information about it.)

[//]: # ()
[//]: # (Below is a minimal example for node classification &#40;supposing we have a graph `G` with a property "myClass"&#41;:)

[//]: # ()
[//]: # (```python)

[//]: # (pipe, _ = gds.beta.pipeline.nodeClassification.create&#40;"myPipe"&#41;)

[//]: # (assert pipe.type&#40;&#41; == "Node classification training pipeline")

[//]: # ()
[//]: # (pipe.addNodeProperty&#40;"degree", mutateProperty="rank"&#41;)

[//]: # (pipe.selectFeatures&#40;"rank"&#41;)

[//]: # (steps = pipe.feature_properties&#40;&#41;)

[//]: # (assert len&#40;steps&#41; == 1)

[//]: # (assert steps[0]["feature"] == "rank")

[//]: # ()
[//]: # (pipe.addLogisticRegression&#40;penalty=&#40;0.1, 2&#41;&#41;)

[//]: # ()
[//]: # (model, res = pipe.train&#40;G, modelName="myModel", targetProperty="myClass", metrics=["ACCURACY"]&#41;)

[//]: # (assert model.metrics&#40;&#41;["ACCURACY"]["test"] > 0)

[//]: # (assert res["trainMillis"] >= 0)

[//]: # ()
[//]: # (res = model.predict_stream&#40;G&#41;)

[//]: # (assert len&#40;res&#41; == G.node_count&#40;&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Link prediction and Node regression works the same way, just with different method names for calls specific to that pipeline.)

[//]: # (Please see the GDS documentation for more on the pipelines' procedure APIs.)

[//]: # ()
[//]: # ()
[//]: # (#### GraphSAGE)

[//]: # ()
[//]: # (Assuming we have a graph `G` with node property `x`, we can do the following:)

[//]: # ()
[//]: # (```python)

[//]: # (model, res = gds.beta.graphSage.train&#40;G, modelName="myModel", featureProperties=["x"]&#41;)

[//]: # (assert len&#40;model.metrics&#40;&#41;["epochLosses"]&#41; == model.metrics&#40;&#41;["ranEpochs"] )

[//]: # (assert res["trainMillis"] >= 0)

[//]: # ()
[//]: # (res = model.predict_stream&#40;G&#41;)

[//]: # (assert len&#40;res&#41; == G.node_count&#40;&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Note that with GraphSAGE we call the `train` method directly and supply all training configuration.)

[//]: # ()
[//]: # ()
[//]: # (### Graph catalog utils)

[//]: # ()
[//]: # (All procedures from the [GDS Graph catalog]&#40;https://neo4j.com/docs/graph-data-science/current/management-ops/graph-catalog-ops/&#41; are supported with `graphdatascience`.)

[//]: # (Some examples are &#40;where `G` is a `Graph`&#41;:)

[//]: # ()
[//]: # (```python)

[//]: # (res = gds.graph.list&#40;&#41;)

[//]: # (assert len&#40;res&#41; == 1  # Exactly one graph is projected)

[//]: # ()
[//]: # (res = gds.graph.streamNodeProperties&#40;G, "rank"&#41;)

[//]: # (assert len&#40;res&#41; == G.node_count&#40;&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Further, there's a call named `gds.graph.get` &#40;`graphdatascience` only&#41;.)

[//]: # (It takes a graph name as input and returns a `Graph` object, if a graph projection of that name exists in the user's graph catalog.)

[//]: # (The idea is to have a way of creating `Graph`s for already projected graphs, without having to do a new projection.)

[//]: # ()
[//]: # ()
[//]: # (### Pipeline catalog utils)

[//]: # ()
[//]: # (All procedures from the [GDS Pipeline catalog]&#40;https://neo4j.com/docs/graph-data-science/current/pipeline-catalog/&#41; are supported with `graphdatascience`.)

[//]: # (Some examples are &#40;where `pipe` is a machine learning training pipeline object&#41;:)

[//]: # ()
[//]: # (```python)

[//]: # (res = gds.beta.pipeline.list&#40;&#41;)

[//]: # (assert len&#40;res&#41; == 1  # Exactly one pipeline is in the catalog)

[//]: # ()
[//]: # (res = gds.beta.pipeline.drop&#40;pipe&#41;)

[//]: # (assert res["pipelineName"] == pipe.name&#40;&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Further, there's a call named `gds.pipeline.get` &#40;`graphdatascience` only&#41;.)

[//]: # (It takes a pipeline name as input and returns a training pipeline object, if a pipeline of that name exists in the user's pipeline catalog.)

[//]: # (The idea is to have a way of creating pipeline objects for already existing pipelines, without having to create them again.)

[//]: # ()
[//]: # ()
[//]: # (### Model catalog utils)

[//]: # ()
[//]: # (All procedures from the [GDS Model catalog]&#40;https://neo4j.com/docs/graph-data-science/current/model-catalog/&#41; are supported with `graphdatascience`.)

[//]: # (Some examples are &#40;where `model` is a machine learning model object&#41;:)

[//]: # ()
[//]: # (```python)

[//]: # (res = gds.beta.model.list&#40;&#41;)

[//]: # (assert len&#40;res&#41; == 1  # Exactly one model is loaded)

[//]: # ()
[//]: # (res = gds.beta.model.drop&#40;model&#41;)

[//]: # (assert res["modelInfo"]["modelName"] == model.name&#40;&#41;)

[//]: # (```)

[//]: # ()
[//]: # (Further, there's a call named `gds.model.get` &#40;`graphdatascience` only&#41;.)

[//]: # (It takes a model name as input and returns a model object, if a model of that name exists in the user's model catalog.)

[//]: # (The idea is to have a way of creating model objects for already loaded models, without having to create them again.)

[//]: # ()
[//]: # ()
[//]: # (### Node matching without Cypher)

[//]: # ()
[//]: # (When calling path finding or topological link prediction algorithms one has to provide specific nodes as input arguments.)

[//]: # (When using the GDS procedure API directly to call such algorithms, typically Cypher `MATCH` statements are used in order to find valid representations of input nodes of interest, see eg. [this example in the GDS docs]&#40;https://neo4j.com/docs/graph-data-science/current/algorithms/dijkstra-source-target/#algorithms-dijkstra-source-target-examples-stream&#41;.)

[//]: # (To simplify this, `graphdatascience` provides a utility function, `gds.find_node_id`, for letting one find nodes without using Cypher.)

[//]: # ()
[//]: # (Below is an example of how this can be done &#40;supposing `G` is a projected `Graph` with `City` nodes having `name` properties&#41;:)

[//]: # ()
[//]: # (```python)

[//]: # (# gds.find_node_id takes a list of labels and a dictionary of)

[//]: # (# property key-value pairs)

[//]: # (source_id = gds.find_node_id&#40;["City"], {"name": "New York"}&#41;)

[//]: # (target_id = gds.find_node_id&#40;["City"], {"name": "Philadelphia"}&#41;)

[//]: # ()
[//]: # (res = gds.shortestPath.dijkstra.stream&#40;G, sourceNode=source_id, targetNode=target_id&#41;)

[//]: # (assert res["totalCost"][0] == 100)

[//]: # (```)

[//]: # ()
[//]: # (The nodes found by `gds.find_node_id` are those that have all labels specified and fully match all property key-value pairs given.)

[//]: # (Note that exactly one node per method call must be matched.)

[//]: # ()
[//]: # (For more advanced filtering we recommend users do matching via Cypher's `MATCH`.)


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
