# Changes in 1.11


## Breaking changes


## New features

* Add the new concept of GDS Sessions, used to manage GDS computations in Aura, based on data from an AuraDB instance.
  * Enables projecting graphs from AuraDB instances to GDS sessions.
  * Add a new top-level class `GdsSessions` to manage GDS sessions in Aura.
    * `GdsSessions` support `get_or_create()`, `list()`, and `delete()`.
  * Creating a new session supports various sizes.
  * The `run_cypher()` method will run Cypher queries targeting the configured AuraDB instance.


## Bug fixes

* Fixed a bug which caused the auth token returned from the GDS Arrow Server was not correctly received.
* Fixed a bug which didn't allow the user to specify `relationship_types` as a string in `gds.graph.relationshipProperties.stream()`.
* Fixed a bug in `kge-predict-transe-pyg-train.ipynb` which now uses the `gds.graph.relationshipProperty.stream()` call and can correctly handle multiple relationships between the same pair of nodes. Issue ref: [#554](https://github.com/neo4j/graph-data-science-client/issues/554)

## Improvements

* Improved the error message if `gds.graph.project.cypher` produces an empty graph.


## Other changes

* Updated required `neo4j` driver from `4.4.2` to the latest 4.4 path release (`4.4.12`) or later.
* Avoid duplications or user-indepedent logs and warnings introduced by the driver option `warn_notification_severity` in `neo4j>=5.21.0`.
