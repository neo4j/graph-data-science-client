# Changes in 1.17

## Breaking changes

## New features

- `sessions.get_or_create()` now supports passing in a manual selection of root certificates for verifying server certificate
- `sessions.get_or_create()` now supports disabling server certificate verification


## Bug fixes

- Fix reporting error based on http responses from the Aura-API with an invalid JSON body. Earlier the client would report JSONDecodeError instead of showing the actual issue.

## Improvements

- `GraphDataScience::run_query` now supports setting the `mode` of the query to be used for routing. Previously queries would always route the leader of the cluster, assuming write mode.
- `GraphDataScience::run_query` now support setting `retryable` to enable a retry-mechanism for appropriate errors. This requires `neo4j>=5.5.0`.


## Other changes
