import sys
from time import time

sys.path.insert(1, "/Users/sbr/graph-data-science-client/")
from graphdatascience import GraphDataScience

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = None
gds = GraphDataScience(NEO4J_URI, auth=NEO4J_AUTH)
gds.set_database("neo4j")

print("Gds server version is: ", gds.version())
print("Gds is licensed: ", gds.is_licensed())
print("Arrow is enabled: ", gds.run_cypher("CALL gds.debug.arrow() YIELD running, enabled"))


def time_graph_construct():
    # print("Loading ogbn-arxiv")
    # t = time()
    # arxiv = gds.graph.ogbn.load("ogbn-arxiv")
    # print(f"ogbn-arxiv loaded in {time() - t}s")

    print("Loading ogbl-wikikg2")
    t = time()
    arxiv = gds.graph.ogbl.load("ogbl-wikikg2")
    print(f"ogbl-wikikg2 loaded in {time() - t}s")

    # print("Loading ogbl-collab")
    # t = time()
    # arxiv = gds.graph.ogbl.load("ogbl-collab")
    # print(f"ogbl-collab loaded in {time() - t}s")

    # print("Loading ogbn-mag")
    # t = time()
    # arxiv = gds.graph.ogbn.load("ogbn-mag")
    # print(f"ogbn-mag loaded in {time() - t}s")


if __name__ == "__main__":
    time_graph_construct()
