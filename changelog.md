# Changes in 1.10


## Breaking changes

* Deprecated support for pyarrow version 10.X.Y as a dependency.


## New features

* Add the new concept of GDS Sessions, used to manage GDS computations in Aura, based on data from an AuraDB instance.
  * Add a new `gds.graph.project` endpoint to project graphs from AuraDB instances to GDS sessions.
    * `nodePropertySchema` and `relationshipPropertySchema` can be used to optimise remote projections.
  * Add a new top-level class `GdsSessions` to manage GDS sessions in Aura.
    * `GdsSessions` support `get_or_create()`, `list()`, and `delete()`.
  * Creating a new session supports various sizes.
  * The `run_cypher()` method will run Cypher queries targetting the configured AuraDB instance.


## Bug fixes

* Fixed an issue where source and target IDs of relationships in heterogeneous OGBL graphs were not parsed correctly.
* Fixed an issue where configuration parameters such as `aggregation` were ignored by `gds.graph.toUndirected`.
* Fixed an issue where the `database` given for the `GraphDataScience` construction was not used for metadata retrieval, causing an exception to be raised if the default "neo4j" database was missing.
* Fixed an issue where progress bars would not always complete.
* Fixed an issue where an empty relationship type could not be streamed.


## Improvements

* Made the first parameter of the `GraphDataScience` constructor, `endpoint`, be a keyword parameter in addition to being a positional parameter.


## Other changes
