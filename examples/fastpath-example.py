from graphdatascience import GraphDataScience

gds = GraphDataScience(
    "neo4j+s://1cec7b2a.databases.neo4j.io",
    auth=("neo4j", "ns0AquKCsQXqL7zTiWRf55DjUK70ZUwkooMorblDt5w"),
    database="neo4j",
)
gds.set_compute_cluster_ip("localhost")

try:
    G = gds.graph.get("cora")
except:
    G = gds.graph.load_cora("cora")

# Optional
graph_filter = {
    "node_labels": {"Paper": ["subject"]},
    "rel_types": {"CITES": []},
}

embeddings = gds.fastpath.stream(
    G,
    graph_filter=graph_filter,
    base_node_label="Paper",
    context_node_label="Paper",
    event_node_label="Paper",
    time_node_property="subject",
    dimension=32,
    num_elapsed_times=7,
    num_output_times=7,
    max_elapsed_time=5,
    mlflow_experiment_name="test",
)
print(embeddings)
