# Changes in 1.10


## Breaking changes

* Deprecated support for pyarrow version 10.X.Y as a dependency.


## New features


## Bug fixes

* Fixed an issue where source and target IDs of relationships in heterogeneous OGBL graphs were not parsed correctly.
* Fixed an issue where configuration parameters such as `aggregation` were ignored by `gds.graph.toUndirected`.
* Fixed an issue where the `database` given for the `GraphDataScience` construction was not used for metadata retrieval, causing an exception to be raised if the default "neo4j" database was missing.
* Fixed an issue where progress bars would not always complete.
* Fixed an issue where an empty relationship type could not be streamed.


## Improvements

* Made the first parameter of the `GraphDataScience` constructor, `endpoint`, be a keyword parameter in addition to being a positional parameter.


## Other changes
