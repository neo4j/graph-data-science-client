# Changes in 2.1

## Breaking changes


## New features

* `GraphDataScience` now disables Arrow server certificate verification when the Bolt URI uses a `+ssc` scheme (`bolt+ssc`, `neo4j+ssc`), unless `disable_server_verification` is set explicitly in `arrow_client_options`. This restores the 1.x behaviour for self-signed certificates, where `arrow_disable_server_verification` defaulted to `True`.

## Bug fixes


## Improvements


## Other changes
