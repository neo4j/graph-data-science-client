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

To use the GDS Python Client, we first need a `gds` object connected to a GDS deployment.
How to get one depends on where GDS runs.


### GDS Sessions (Aura Graph Analytics)

To run GDS via [Aura Graph Analytics](https://neo4j.com/docs/aura/graph-analytics/), use `GdsSessions` with an Aura API key pair and your AuraDB connection information:

```python
from graphdatascience.session import AuraAPICredentials, DbmsConnectionInfo, GdsSessions, SessionMemory

sessions = GdsSessions(api_credentials=AuraAPICredentials("my-client-id", "my-client-secret"))

# Optional: estimate the session memory needed for your workload
memory = sessions.estimate(node_count=2_000, relationship_count=5_000, algorithms=["pageRank"])

gds = sessions.get_or_create(
    session_name="my-session",
    memory=memory,  # or a fixed size, e.g. SessionMemory.m_4GB
    db_connection=DbmsConnectionInfo(
        "neo4j+s://my-aura-db.databases.neo4j.io",
        "neo4j",
        "my-password",
    ),
)

# ... project graphs, run algorithms, train models, see the usage example below ...

# When you are done, tear down the session
gds.delete()
# or: sessions.delete(session_name="my-session")
```

Sessions can also run standalone, without an attached AuraDB, by passing a `cloud_location` instead of a `db_connection`.
See the [GDS Python Client Manual](https://neo4j.com/docs/graph-data-science-client/current/) for details.


### Self-managed Neo4j or GDS plugin

If you connect to a self-managed Neo4j database with the [GDS plugin](https://neo4j.com/docs/graph-data-science/current/installation/neo4j-server/) installed, instantiate a `GraphDataScience` object directly:

```python
from graphdatascience import GraphDataScience

# When connecting to an AuraDS instance, the client automatically applies the AuraDS-recommended driver settings
gds = GraphDataScience("neo4j+s://my-aura-ds.databases.neo4j.io:7687", auth=("neo4j", "my-password"))

# Import the Cora dataset to GDS
G = gds.graph.datasets.load_cora()
assert G.node_count() == 2_708

# Run PageRank in mutate mode on G
pagerank_result = gds.page_rank.mutate(G, tolerance=0.5, mutate_property="pagerank")
assert pagerank_result["node_properties_written"] == G.node_count()

# Create a Node Classification pipeline
pipeline, _ = gds.pipeline.node_classification.create("my-pipe")

# Add a Degree Centrality feature to the pipeline
pipeline.add_node_property("degree", mutate_property="rank")
pipeline.select_features("rank")
details = pipeline.details()
assert details.feature_properties == [{"feature": "rank"}]

# Add a training method
pipeline.add_logistic_regression(penalty=(0.1, 2))

# Train a model on G
model, train_result = pipeline.train(
    G, model_name="my-model", target_property="subject", metrics=["ACCURACY"]
)
assert model.metrics()["ACCURACY"]["test"] > 0
assert train_result.train_millis >= 0

# Compute predictions in stream mode
predictions = model.predict_stream(G)
assert len(predictions) == G.node_count()
```

For additional examples and extensive documentation of all capabilities, please refer to the [GDS Python Client Manual](https://neo4j.com/docs/graph-data-science-client/current/).

Full end-to-end examples in Jupyter ready-to-run notebooks can be found in the [`examples` source directory](https://github.com/neo4j/graph-data-science-client/tree/main/examples):

* [Machine learning pipelines: Node classification](examples/ml-pipelines-node-classification.ipynb)
* [Node Regression with Subgraph and Graph Sample projections](examples/node-regression-with-subgraph-and-graph-sample.ipynb)
* [Product recommendations with kNN based on FastRP embeddings](examples/fastrp-and-knn.ipynb)
* [Sampling, Export and Integration with PyG example](examples/import-sample-export-gnn.ipynb)
* [Load data to a projected graph via graph construction](examples/load-data-via-graph-construction.ipynb)
* [Heterogeneous Node Classification with HashGNN and Autotuning](https://github.com/neo4j/graph-data-science-client/tree/main/examples/heterogeneous-node-classification-with-hashgnn.ipynb)
* [Perform inference using pre-trained KGE models](examples/kge-predict-transe-pyg-train.ipynb)


## Documentation

The primary source for learning everything about the GDS Python Client is the manual, hosted at https://neo4j.com/docs/graph-data-science-client/current/.
The manual is versioned to cover all GDS Python Client versions, so make sure to use the correct version to get the correct information.


## Known limitations

Operations known to not yet work with `graphdatascience`:

* [Numeric utility functions](https://neo4j.com/docs/graph-data-science/current/management-ops/utility-functions/#utility-functions-numeric) (will never be supported)


## License

`graphdatascience` is licensed under the Apache Software License version 2.0.
All content is copyright © Neo4j Sweden AB.


## Acknowledgements

This work has been inspired by the great work done in the following libraries:

* [pygds](https://github.com/stellasia/pygds) by stellasia
* [gds-python](https://github.com/moxious/gds-python) by moxious
