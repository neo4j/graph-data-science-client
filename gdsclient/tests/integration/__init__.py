import os

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")

if os.environ.get("NEO4J_USER") is not None:
    AUTH = (
        os.environ.get("NEO4J_USER", "neo4j"),
        os.environ.get("NEO4J_PASSWORD", "neo4j"),
    )
else:
    AUTH = None
