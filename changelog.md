# Changes in 1.22

## Breaking changes

* Removed graph_name parameter from `graphdatascience.arrow_client.v2.gds_arrow_client.GdsArrowClient.stream_job`.

## New features


## Bug fixes

* Fixed a bug where `gds.v2.graph.construct` and `gds.v2.graph.datasets` would fail to import heterogeneous graphs.
* Fixed a bug where `graphdatascience.arrow_client.v2.gds_arrow_client.GdsArrowClient.stream_job` would start a new job instead of only streaming an existing job.


## Improvements


## Other changes
