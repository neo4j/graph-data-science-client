# Changes in 2.1

## Breaking changes


## New features

* `GraphDataScience` now disables Arrow server certificate verification when the Bolt URI uses a `+ssc` scheme (`bolt+ssc`, `neo4j+ssc`), unless `disable_server_verification` is set explicitly in `arrow_client_options`. This restores the 1.x behaviour for self-signed certificates, where `arrow_disable_server_verification` defaulted to `True`.

## Bug fixes

* Deriving the default project for GDS Sessions now first checks the users organizations. Users with access to multiple organizations must specify `project_id` explicitly to have a single project and avoid silently create sessions in the wrong one.

## Improvements

* `QueryMode` parameters, such as `gds.run_cypher(mode=...)`, now also accept plain strings (`"READ"`/`"WRITE"`, case-insensitive)
* `gds.graph.relationships.to_undirected(aggregation=...)` now also accepts plain strings, including as per-property dictionary values
* Topological link prediction `direction` parameters now also accept plain strings (`"OUTGOING"`/`"INCOMING"`/`"BOTH"`)
* When gRPC fails to resolve the Arrow server host but the operating system resolver can (typically behind local DNS proxies such as Cloudflare WARP), the client now raises an error pointing at the `GRPC_DNS_RESOLVER=native` workaround instead of the bare `FlightUnavailableError`.

## Other changes
