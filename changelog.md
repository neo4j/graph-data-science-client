# Changes in 2.1

## Breaking changes


## New features


## Bug fixes

* Deriving the default project for GDS Sessions now enumerates projects across all organizations via the Aura API `v2beta1` endpoints. Previously the non-organization-aware `v1/tenants` endpoint could make accounts with multiple organizations appear to have a single project, silently creating sessions in the wrong project.

## Improvements


## Other changes
