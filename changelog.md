# Changes in 1.16

## Breaking changes

* Drop support for PyArrow 16

## New features

## Bug fixes

- Fixed a bug where remote projections would fail when the database is clustered
- Fixed a bug where the validation of gds.graph.project.cypher would fail when the return clause was not uppercase

## Improvements

- Allow creating sessions of size `512GB`.
- Allow passing additional parameters for the Neo4j driver connection to `GdsSessions.get_or_create(neo4j_driver_config={..})`
- Add helper functions to create config objects from environment variables
  - `AuraApiCredentials::from_env`
  - `DbmsConnectionInfo::from_env`
- Retry internal functions known to be idempotent. Reduces issues such as `SessionExpiredError`.
- Add support for PyArrow 20

## Other changes
