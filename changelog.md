# Changes in 1.11


## Breaking changes


## New features

* Add the new concept of GDS Sessions, used to manage GDS computations in Aura, based on data from an AuraDB instance.
  * Add a new `gds.graph.project` endpoint to project graphs from AuraDB instances to GDS sessions.
    * `nodePropertySchema` and `relationshipPropertySchema` can be used to optimise remote projections.
  * Add a new top-level class `GdsSessions` to manage GDS sessions in Aura.
    * `GdsSessions` support `get_or_create()`, `list()`, and `delete()`.
  * Creating a new session supports various sizes.
  * The `run_cypher()` method will run Cypher queries targetting the configured AuraDB instance.


## Bug fixes

* Fixed a bug which caused the auth token returned from the GDS Arrow Server was not correctly received.

## Improvements


## Other changes
