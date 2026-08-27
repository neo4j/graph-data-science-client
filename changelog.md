# Changes in 2.0

## Breaking changes

* All untyped 1.x endpoints have been removed. The V2 API, previously available under the `gds.v2` prefix, is now the default and only API of the client, and the `gds.v2` prefix is gone. See the migration guide for how to port existing code: https://neo4j.com/docs/graph-data-science-client/current/migration-from-1x/
* The minimum supported GDS server version is now `2.13.0`.
* The minimum supported Neo4j Python driver version is now `5.26.0`. The Neo4j `4.4` driver is no longer supported.
* Pandas `1.x` is no longer supported. The supported range is now `pandas >= 2.0, < 4.0`, which includes Pandas `3.0`.
* The supported `pyarrow` range is now `>= 21.0, < 26.0`, and the supported `numpy` range is now `< 3.0`.
* The `failIfMissing` parameter of `Graph.drop` and `Model.drop` was renamed to `fail_if_missing`.
* `ServerVersion` and `SemanticVersion` have moved to the `graphdatascience.versions` package.
* The `write_concurrency` and `job_id` parameters have been removed from `gds.graph.node_labels.mutate`.
* `ArrowEndpointVersion.from_arrow_info` has been replaced by `check_version_compatibility`.
* The `retryable` parameter of `run_cypher` has been removed. Cypher queries run via `run_cypher` now always use transactional retries.
* `GraphV2` was renamed to `Graph` and `ModelV2` was renamed to `Model`. `Graph` is now exported at the top level, so `from graphdatascience import Graph` works.
* For Aura Graph Analytics `gds.graph.project` was renamed to `gds.graph.project.cypher`
* For Aura Graph Analytics `gds.graph.project_native` was renamed to `gds.graph.project.native`
* Provide reason for deleted state of a deleted GDS Sessions
* The `database` parameter has been removed from `gds.graph.project.cypher` and `gds.graph.project.cypher_async`. The database is now always sourced from the gds object; use `gds.set_database()` to change it.

## New features

* Added an `algorithms` parameter to `GdsSessions.estimate` to estimate the memory for individual algorithms and their configuration, instead of whole algorithm categories. The parameter accepts either a list of algorithm names or a mapping from algorithm name to configuration. Names are matched case-insensitively, the `gds.` prefix is optional, and Python endpoint names such as `node_embedding.fastrp` are mapped to their GDS procedure names.
* Added `gds.pipeline.get` to retrieve a pipeline from the pipeline catalog, and pipeline objects can now return their pipeline info.
* `GdsSessions.get_or_create` now accepts a `show_progress` parameter to control whether the returned client prints its own progress bars.
* Added an optional `overwrite` parameter to `gds.graph.project`, `gds.graph.generate`, `gds.graph.construct`, `gds.graph.filter` and `gds.graph.sample` (and their async/session variants). When set to `True`, an existing graph with the same name is dropped before the new graph is created.
* Expose `gds.fast_path` for Aura Graph Analytics / GDS Sessions. 

## Bug fixes

* Requests to the Aura API that are rejected as unauthorized are now retried once with a new OAuth token, instead of failing permanently while the expired token stays cached
* Requests to the Aura API now use a connect and read timeout, so that a stalled connection can no longer block a call indefinitely.
* `GdsSessions.delete` now returns `False` if no session was deleted when called with a `session_id`. It previously always returned `True`.
* `AuraApiError` and `SessionStatusError` no longer include a repetition of the exception object in their message.
* The Arrow endpoint version is now checked when the client is created. If it is unsupported, the client raises an error asking to update the `graphdatascience` package, instead of failing later with an unrelated error.
* Getting a session that has already expired now raises a `RuntimeError` with a human-readable duration, instead of warning with a wrapped (and misleading) `timedelta.seconds` value. Sessions expiring within the next hour still emit a warning.
* `GraphDataScience.close()` now closes the Arrow Flight client in addition to the query runner.
* `gds.graph.construct` now correctly forwards the `inverse_indexed_relationship_types`.

## Improvements

* `GraphDataScience` no longer requires the `aura_ds` parameter to be set. If left unset, the client automatically derives whether the database is hosted in Aura.
* `gds.project.cypher` will automatically rewrite queries that contain `gds.graph.project` instead of `gds.graph.project.remote`
* `gds.project.cypher` will check if `undirectedRelationshipTypes` and `inverseIndexedRelationshipTypes` are defined in the projection query instead of the method parameters.
* The `show_progress` setting of a client is now honoured consistently: graph projections, `WriteJobHandle.wait` and the node property endpoints all inherit it. `ProgressBar.set_default_options` allows setting process-wide progress bar options.
* Estimation errors and documentation now refer to Python endpoint names instead of GDS procedure names.
* Session errors, such as a session failing with an out-of-memory error, are now reported together with the session status. They are also surfaced automatically when an operation fails because the session can no longer be reached, instead of only reporting the underlying connection error.
* Improve error message if `gds.graph.project.cypher` produced no graph due to no matching data.

## Other changes

* The documentation and the example notebooks have been reworked for the 2.0 API. A new migration guide describes how to move from the 1.x series: https://neo4j.com/docs/graph-data-science-client/current/migration-from-1x/
