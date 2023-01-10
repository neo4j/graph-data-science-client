# Changes in 1.6


## Breaking changes


## New features

* Added a new parameter `undirected_relationship_types` to `gds.alpha.graph.construct` which allows constructing undirected graphs, when using GDS >= 2.3.0.
* Added a new parameter `undirected` to `gds.load_cora` to load the dataset undirected.
* Added new method `gds.alpha.graph.nodeLabel.write` to write back node labels to Neo4j database.
* Added new convenience methods to the `Model` object:
  * `model_info` to get model metadata obtained during training.
  * `classes` to list all classes used during training (only for Node Classification models).
  * `best_parameters` which returns a pandas `Series` containing the parameters of the model candidate winning the model selection training.
  * `feature_properties` (only for Node Property models)
  * `link_features` (only for LP models)
  * `node_property_steps`
* Added new convenience factory methods to create pipeline objects.
* `gds.graph.construct` now renders a progress bar if Arrow support is enabled.
* Added a new method `gds.graph.relationships.to_undirected` to turn a directed relationship type to an undirected, when using GDS >= 2.3.0
* Added new pre-canned datasets:
  * `gds.graph.load_karate_club`
  * `gds.graph.load_imdb`
* Added new method `gds.alpha.graph.nodeLabel.mutate` to mutate the in-memory graph with new node labels.


## Bug fixes


## Improvements

* Improved `Model.metrics()` method for pipeline models (e.g. LP, NC, NR) to return custom type.
* Improved `gds.graph.construct()` to support multiple dataframes for nodes and relationships without arrow.

## Other changes
