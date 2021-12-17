from gdsclient import QueryRunner


class CollectingQueryRunner(QueryRunner):
    def __init__(self):
        self.queries = []
        self.params = []

    def run_query(self, query, params={}):
        self.queries.append(query)
        self.params.append(params)
