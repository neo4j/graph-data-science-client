# Changes in 1.6


## Breaking changes


## New features

* Added a new parameter `undirected_relationship_types` to `gds.alpha.graph.construct` which allows constructing undirected graphs, when using GDS >= 2.3.0.
* Added a new parameter `undirected` to `gds.load_cora` to load the dataset undirected.
* Added new method `gds.alpha.graph.nodeLabel.write` to write back node labels to Neo4j database.
* Added new convenience methods to the `Model` object:
  * `model_info` to get model metadata obtained during training.
  * `classes` to list all classes used during training (only for Node Classification models).
  * `best_parameters` which returns a pandas `Series` containing the parameters of the model winning the model selection training.
  * `pipeline` which returns information about the pipeline steps that are part of running predictions with the model.
* Added new convenience factory methods to create pipeline objects.


## Bug fixes


## Improvements

* Improved `Model.metrics()` method for pipeline models (e.g. LP, NC, NR) to return custom type.


## Other changes
