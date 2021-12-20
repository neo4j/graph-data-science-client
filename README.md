# gdsclient

This repo hosts the sources for `gdsclient`, a Python wrapper API for operating and working with the [Neo4j Graph Data Science (GDS) library](https://github.com/neo4j/graph-data-science).
gdsclient enables users to write pure Python code to project graphs, run algorithms, and define and use machine learning pipelines in GDS.
The API is designed to mimic the GDS Cypher procedure API, but in Python code.
It abstracts the necessary operation of the Neo4j Python Driver to offer a simpler surface.

Please leave any feedback as issues on this repository.
Happy coding!


## Installation

To build and install `gdsclient` from this repository, simply run the following command:

```bash
pip3 install .
```


## Documentation

Refer to the [GDS Manual](https://neo4j.com/docs/graph-data-science/current/).


## Acknowledgements

This work has been inspired by the great work done in the following libraries:

* [pygds](https://github.com/stellasia/pygds) by stellasia
* [gds-python](https://github.com/moxious/gds-python) by moxious


## License

See LICENSE file.
All content is copyright © Neo4j Sweden AB.
