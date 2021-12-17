from gdsclient import GDS, QueryRunner


def test_create_graph_native():
    runner = CollectingQueryRunner()
    gds = GDS(runner)
    graph = gds.graph.create("g", "A", "R")
    assert graph
    assert runner.queries == [
        "CALL gds.graph.create($graph_name, $node_projection, $relationship_projection)"
    ]
    assert runner.params == [
        {"graph_name": "g", "node_projection": "A", "relationship_projection": "R"}
    ]


class CollectingQueryRunner(QueryRunner):
    def __init__(self):
        self.queries = []
        self.params = []

    def run_query(self, query, params={}):
        self.queries.append(query)
        self.params.append(params)
