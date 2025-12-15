# Changes in 1.19

## Breaking changes

## New features

## Bug fixes

## Improvements

- `GdsSessions.get_or_create` now allows to specify the `aura_instance_id` instead of `uri` as part of the `db_connection`. This is required if the instance id could not be derived from the provided database connection URI such as for Multi-Database instances.
- `GdsSessions.estimate` now recommends smaller sizes such as `2GB`. Also allows specifying property and label counts for better estimates.

## Other changes

- Deprecate deriving `aura_instance_id` from provided `uri` for `GdsSessions.get_or_create`.
