# Changes in 1.15


## Breaking changes


## New features


## Bug fixes


## Improvements

* Reduce calls to the Aura API during GdsSessions::get_or_create.
* Improve error message when a query is interrupted by a signal (SIGINT or SIGTERM).
* Improve error message if session is expired.


## Other changes

* Aura API credentials will now be used to authenticate with the Session
