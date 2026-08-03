# Changes in 2.0

## Breaking changes

* The minimum supported Neo4j Python driver version is now `5.26.0`. The Neo4j `4.4` driver is no longer supported.
* The `retryable` parameter of `run_cypher` has been removed. Cypher queries run via `run_cypher` now always use transactional retries.
* For Aura Graph Analytics `gds.graph.project` was renamed to `gds.graph.project.cypher`
* For Aura Graph Analytics `gds.graph.project_native` was renamed to `gds.graph.project.native`

## New features

* Added an `algorithms` parameter to `GdsSessions.estimate` to estimate the memory for individual algorithms and their configuration, instead of whole algorithm categories.


## Bug fixes

* Requests to the Aura API that are rejected as unauthorized are now retried once with a newly minted OAuth token, instead of failing permanently while the expired token stays cached. This affected operations such as `GdsSessions.delete`, which could leave a session running.
* Requests to the Aura API now use a connect and read timeout, so that a stalled connection can no longer block a call indefinitely. The number of retries was also reduced, so that a request is less likely to be retried for longer than its OAuth token stays valid.
* `GdsSessions.delete` now returns `False` if no session was deleted when called with a `session_id`. It previously always returned `True`.
* `AuraApiError` and `SessionStatusError` no longer include a repetition of the exception object in their message.


## Improvements

* `GraphDataScience` no longer requires the `aura_ds` parameter to be set. If left unset, the client automatically derives whether the database is hosted in Aura.
* `gds.project.cypher` will automatically rewrite queries that contain `gds.graph.project` instead of `gds.graph.project.remote`
* `gds.project.cypher` will check if `undirectedRelationshipTypes` and `inverseIndexedRelationshipTypes` are defined in the projection query instead of the method parameters.


## Other changes
