# Changes in 1.21

## Breaking changes


## New features

* Support fail_if_missing parameter for `gds.v2.model.delete`


## Bug fixes

* Fixed a bug where `gds.v2.graph.list()` would fail due to `memoryUsage` not being present in the result.
* Fixed a bug where GDS would fail if constructed via `from_driver` and without explicit auth provided.
* Fixed a bug in `gds.v2.model.drop` where it would fail even if `fail_if_missing` was set to `False`. 
* Fixed a bug in `gds.v2.graph.generate` where it would fail if the `relationship_property` was not set.


## Improvements

* Set `app` and `type` in Cypher queries being sent to Neo4j. This allows for better analysis by tools such as Neo4j Ops manager.
* Warn about falling back to Cypher instead of using Arrow, if GraphDataScience object is created via `from_driver` without explict auth. 
* Added new optional filters to `GdsSessions::list`, such as `instance_id`, `list_only_owned` or `include_deleted`.


## Other changes
