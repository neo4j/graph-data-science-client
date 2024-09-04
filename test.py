import os

from graphdatascience import GdsSessions
from graphdatascience.session import AuraAPICredentials

os.environ["AURA_ENV"] = "staging"

sessions = GdsSessions(
    api_credentials=AuraAPICredentials(
        client_id="eTRPZMyxHS3eeCyagyS6uzxuGtwTAMQX",
        client_secret="65bplhdJFI3CevjA5jxMXgDV1OkLQtQko9G2raqITNloJuxtMSejiOsL8IilsFTQ",
        tenant_id="95f9dd48-8439-4cb8-bf3d-7100923d8fba",
    )
)
# gds = sessions.get_or_create(
#     "foo",
#     SessionMemory.m_8GB,
#     DbmsConnectionInfo(uri="bolt://localhost:7687", username="neo4j", password="password"),
#     CloudLocation("gcp", "europe-west1"),
# )

print(sessions.list())

# G, result = gds.graph.project(
#     "people-and-fruits",
#     """
#     CALL {
#         MATCH (p1:Person)
#         OPTIONAL MATCH (p1)-[r:KNOWS]->(p2:Person)
#         RETURN
#           p1 AS source, r AS rel, p2 AS target,
#           p1 {.age, .experience, .hipster } AS sourceNodeProperties,
#           p2 {.age, .experience, .hipster } AS targetNodeProperties
#         UNION
#         MATCH (f:Fruit)
#         OPTIONAL MATCH (f)<-[r:LIKES]-(p:Person)
#         RETURN
#           p AS source, r AS rel, f AS target,
#           p {.age, .experience, .hipster } AS sourceNodeProperties,
#           f { .tropical, .sourness, .sweetness } AS targetNodeProperties
#     }
#     RETURN gds.graph.project.remote(source, target, {
#       sourceNodeProperties: sourceNodeProperties,
#       targetNodeProperties: targetNodeProperties,
#       sourceNodeLabels: labels(source),
#       targetNodeLabels: labels(target),
#       relationshipType: type(rel)
#     })
#     """,
# )

# G = gds.graph.get("people-and-fruits")

# print(gds.wcc.stream(G))
