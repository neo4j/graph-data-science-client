from graphdatascience.session.aura_client import AuraClient


def test_aura_client():
    client = AuraClient(
        "",
        "",
        "neo4j+s://localhost:7687",
        "neo4j",
        "12345678",
    )

    sessions = client.list_sessions()

    print(sessions)
