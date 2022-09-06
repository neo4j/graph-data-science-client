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


## Improvements


## Other changes
