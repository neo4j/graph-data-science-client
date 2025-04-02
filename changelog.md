# Changes in 1.15


## Breaking changes

* Rename `tenant_id` to `project_id` in class `graphdatascience.session.AuraAPICredentials`.


## New features

* Allow the creation of fully standalone GDS Sessions by omitting the database connection information

## Bug fixes


## Improvements

* Reduce calls to the Aura API during GdsSessions::get_or_create.
* Improve error message when a query is interrupted by a signal (SIGINT or SIGTERM).
* Improve error message if session is expired.
* Improve robustness of Arrow client against connection errors such as `FlightUnavailableError` and `FlightTimedOutError`.
* Return dedicated error class `SessionStatusError` if a session failed or expired.


## Other changes

* Aura API credentials will now be used to authenticate with the Session
