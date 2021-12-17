from gdsclient import QueryRunner


class CollectingQueryRunner(QueryRunner):
    def __init__(self):
        self.queries = []
        self.params = []

    def run_query(self, query, params={}):
        self.queries.append(query)
        self.params.append(params)

    def last_query(self):
        return self.queries[-1]

    def last_params(self):
        return self.params[-1]
