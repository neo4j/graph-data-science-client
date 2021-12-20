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

A minimal example of using `gdsclient` to connect to a Neo4j database and run GDS algorithms:

```python
from neo4j import GraphDatabase
from gdsclient import Neo4jQueryRunner, GraphDataScience

# Set up driver and gds module
URI = "bolt://localhost:7687" # Override according to your setup
driver = GraphDatabase.driver(URI) # You might also have auth set up in your db
runner = Neo4jQueryRunner(driver)
gds = GraphDataScience(runner)

# Project your graph
graph = gds.graph.create("graph", "*", "*")

# Run the PageRank algorithm with custom configuration
gds.pageRank.write(graph, tolerance=0.5, writeProperty="pagerank")
```

For extensive documentation of all operations supported by GDS, please refer to the [GDS Manual](https://neo4j.com/docs/graph-data-science/current/).

A full end-to-end example of using KNN and FastRP with multiple execution modes in a Jupyter ready-to-run notebook can be found in the `examples` directory.


## Acknowledgements

This work has been inspired by the great work done in the following libraries:

* [pygds](https://github.com/stellasia/pygds) by stellasia
* [gds-python](https://github.com/moxious/gds-python) by moxious


## License

See LICENSE file.
All content is copyright © Neo4j Sweden AB.
