# Changes in 1.8


## Breaking changes


## New features

* New method `gds.graph.cypher.project` to project a graph using Cypher projection.
  More details can be found in the user guide.
* Added new LastFM dataset through `gds.graph.load_lastfm()`.
* Expose bookmarks to synchronize queries in a Neo4j cluster.
* Added `SimpleRelEmbeddingModel` relationship embedding model class for predicting new relationships based on a pre-trained KGE model. It can be created via the new methods:
    * `gds.model.transe.create` for models trained with the TransE scoring function, and
    * `gds.model.distmult.create` for models trained with the DistMult scoring function.
* The new `SimpleRelEmbeddingModel` has three methods for predicing relationships:
    * `predict_stream` which streams back the predicted relationships,
    * `predict_mutate` which adds the predicted relationships to the projected graph, and
    * `predict_write` which writes back the predicted relationships to the Neo4j database.
* The new `SimpleRelEmbeddingModel` has four getter methods for inspecting it:
    * `scoring_function` which return the scoring function of the model
    * `graph_name` which returns the name of the graph the model is based on
    * `node_embedding_property` which returns the name of the node property storing embeddings in the graph
    * `relationship_type_embeddings` which returns the relationship type embeddings of the model


## Bug fixes

* Fixed a bug, where the graph object would list multiple graphs if the same name was used for graphs on different databases.


## Improvements

* Improved endpoint returning graphs to be used in `with` clauses. The expression `with gds.graph.project(...)[0] as G` can now be simplified to `with gds.graph.project(...) AS G`.
* Improve the error message if GDS is not correctly installed on the server.
* Forward previously ignored Cypher warnings as Python warnings. This includes for instance deprecation warnings.
* Make `gds.graph.construct` more robust by ignoring empty dataframes inside. This makes it less error-prone to construct nodes only graphs.


## Other changes

* Dropped support for Python 3.7 which is EOL.

