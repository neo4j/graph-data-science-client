# Changes in 1.14


## Breaking changes


## New features


## Bug fixes

* Fixed a bug, where sessions could not be created for AuraDB instances of tier `business-critical`.
* Fixed a bug, where sessions would fail on write-back if the Graph was empty.
* Fixed a bug, where the progress bar would not be shown unless the `jobId` parameter was set. The progress bar can be toggled on and off via `GraphDataScience::set_show_progress`.


## Improvements

* Display progress bar for remote projection and open-ended tasks.
* Improve progress bar by showing currently running task.
* Allow passing the optional graph filter also as type `str` to `gds.graph.list()` instead of only `Graph`.


## Other changes
