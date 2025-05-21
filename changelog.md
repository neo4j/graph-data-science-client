# Changes in 1.16


## Breaking changes

## New features

## Bug fixes

* Fixed a bug where remote projections would fail when the database is clustered

## Improvements

* Allow creating sessions of size `512GB`.
* Allow passing additional parameters for the Neo4j driver connection to `GdsSessions.get_or_create(neo4j_driver_config={..})`


## Other changes
