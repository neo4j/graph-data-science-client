# Changes in 2.4

## Breaking changes


## New features

- Added a `result_as_vector` parameter to the `stream`, `write`, and `mutate` endpoints of the
  embedding algorithms (`fast_rp`, `node2vec`, `hash_gnn`, and `graph_sage`), as well as their
  session `compute` endpoints. When enabled, the embeddings are produced as Neo4j `VECTOR`
  values instead of lists of floats. The parameter requires GDS 2026.11 or later.

## Bug fixes

* Fixed `gds.graph.relationships.write` ignoring the `write_concurrency` parameter when running against a Neo4j database with GDS installed.

## Improvements

- The session algorithm endpoints now expose the compute-only endpoint (`gds.<algorithm>.compute`)
  as part of their documented interface. It starts a computation as a job without materializing
  its result, in addition to the algorithm modes.
- Improve robustness of Neo4j DB connection verification.
- `QueryType`, `AlgorithmCategory`, `ArrowEndpointVersion`, and `ProtocolVersion` are now
  `StrEnum`s: `str()` of a member returns the underlying value (for example,
  `str(QueryType.USER_ACTION)` is `"user-action"` instead of `"QueryType.USER_ACTION"`),
  and members compare equal to their string values.

## Other changes
