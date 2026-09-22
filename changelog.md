# Changes in 2.1

## Breaking changes


## New features


## Bug fixes

* Deriving the default project for GDS Sessions now first checks the account's organizations via the Aura API `v2beta1` endpoint. Accounts with access to multiple organizations must specify `project_id` explicitly, since the non-organization-aware `v1/tenants` endpoint could make them appear to have a single project and silently create sessions in the wrong one.

## Improvements


## Other changes
