# Changes in 2.0

## Breaking changes

* The minimum supported Neo4j Python driver version is now `5.26.0`. The Neo4j `4.4` driver is no longer supported.
* The `retryable` parameter of `run_cypher` has been removed. Cypher queries run via `run_cypher` now always use transactional retries.
* For Aura Graph Analytics `gds.graph.project` was renamed to `gds.graph.project.cypher`
* For Aura Graph Analytics `gds.graph.project_native` was renamed to `gds.graph.project.native`

## New features


## Bug fixes


## Improvements

* `GraphDataScience` no longer requires the `aura_ds` parameter to be set. If left unset, the client automatically derives whether the database is hosted in Aura.
* `gds.project.cypher` will automatically rewrite queries that contain `gds.graph.project` instead of `gds.graph.project.remote`
* `gds.project.cypher` will check if `undirectedRelationshipTypes` and `inverseIndexedRelationshipTypes` are defined in the projection query instead of the method parameters.


## Other changes
