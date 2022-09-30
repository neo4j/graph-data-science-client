# Changes in 1.4


## Breaking changes


## New features

* The `DataFrame` returned by `gds.beta.graph.relationships.stream` now has a convenience method called `by_rel_type`.
This method converts the topology result to a map from relationship types to matrices describing the topology of the type.
* Added a new optional string parameter `database` to `GraphDataScience.run_cypher` for overriding which database to target.
* Added new method `gds.graph.load_cora` to load the CORA dataset into GDS.


## Bug fixes

* Fix resolving Node regression pipelines created via `gds.alpha.pipeline.nodeRegression.create`.
* Fix resolving Node regression models created via `gds.alpha.pipeline.nodeRegression.train`.
* Fix an issue where `run_cypher` did not execute Cypher correctly in some edge cases.


## Improvements

* Added custom implementation of `__str__` and `__repr__` for Graph, Model and Pipeline objects.
* Added a more helpful custom exception message for when a `str` is provided to methods that take `Graph` objects.


## Other changes
