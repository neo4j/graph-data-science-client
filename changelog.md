# Changes in 1.21

## Breaking changes


## New features


## Bug fixes

* Fixed a bug where `gds.v2.graph.list()` would fail due to `memoryUsage` not being present in the result.
* Fixed a bug where GDS would fail if constructed via `from_driver` and without explicit auth provided.


## Improvements

* Set `app` and `type` in Cypher queries being sent to Neo4j. This allows for better analysis by tools such as Neo4j Ops manager.
* Warn about falling back to Cypher instead of using Arrow, if GraphDataScience object is created via `from_driver` without explict auth. 


## Other changes
