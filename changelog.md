# Changes in 1.7


## Breaking changes


## New features

* Add a new method `GraphDataScience.server_version` which returns the version of the server not as a `str` but as a `ServerVersion`. This allows easier inspection of the major, minor and patch version.
* Implement context managment protocol for `Graph`. This allows usage as part of the `with` statements, where the graph is dropped at the end.
* Added possibility to load datasets from the Open Graph Benchmark via the new methods:
	* `gds.graph.ogbn.load` for node property prediction datasets, and
	* `gds.graph.ogbl.load` for link property prediction datasets.
* Added possibility to load NetworkX graphs via the new method `gds.graph.networkx.load`.


## Bug fixes


## Improvements

* When an almost correct method is called, raise an error with a message that suggests the most probable correct method name that was intended.
* Improved IDE auto-completion support to give significantly fewer false positive suggestions.
* Failing to log progress of a call will no longer fail the call itself, but just warn that logging was unsuccessful.
* Underlying connections to a Neo4j DBMS is now being verified and retried automatically up to a timeout of 10 minutes.
* The `GraphDataScience.from_neo4j_driver` factory method now additionally takes the same Arrow related keyword parameters as the `GraphDataScience` constructor.


## Other changes
