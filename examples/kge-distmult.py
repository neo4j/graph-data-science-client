import collections
import os

from neo4j.exceptions import ClientError
from tqdm import tqdm

from graphdatascience import GraphDataScience

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = None
NEO4J_DB = os.environ.get("NEO4J_DB", "neo4j")
if os.environ.get("NEO4J_USER") and os.environ.get("NEO4J_PASSWORD"):
    NEO4J_AUTH = (
        os.environ.get("NEO4J_USER"),
        os.environ.get("NEO4J_PASSWORD"),
    )
gds = GraphDataScience(NEO4J_URI, auth=NEO4J_AUTH, database=NEO4J_DB, arrow=True)


try:
    _ = gds.run_cypher("CREATE CONSTRAINT entity_id FOR (e:Entity) REQUIRE e.id IS UNIQUE")
except ClientError:
    print("CONSTRAINT entity_id already exists")

import os
import zipfile
from collections import defaultdict

from ogb.utils.url import download_url

url = "https://download.microsoft.com/download/8/7/0/8700516A-AB3D-4850-B4BB-805C515AECE1/FB15K-237.2.zip"
raw_dir = "./data_from_zip"
download_url(f"{url}", raw_dir)

raw_file_names = ["train.txt", "valid.txt", "test.txt"]
with zipfile.ZipFile(raw_dir + "/" + os.path.basename(url), "r") as zip_ref:
    for filename in raw_file_names:
        zip_ref.extract(f"Release/{filename}", path=raw_dir)
data_dir = raw_dir + "/" + "Release"

rel_types = {
    "train.txt": "TRAIN",
    "valid.txt": "VALID",
    "test.txt": "TEST",
}
rel_id_to_text_dict = {}
rel_type_dict = collections.defaultdict(list)
rel_dict = {}


def read_data():
    node_id_set = {}
    dataset = defaultdict(lambda: defaultdict(list))
    for file_name in raw_file_names:
        file_name_path = data_dir + "/" + file_name

        with open(file_name_path, "r") as f:
            data = [x.split("\t") for x in f.read().split("\n")[:-1]]

        for i, (src_text, rel_text, dst_text) in enumerate(data):
            if src_text not in node_id_set:
                node_id_set[src_text] = len(node_id_set)
            if dst_text not in node_id_set:
                node_id_set[dst_text] = len(node_id_set)
            if rel_text not in rel_dict:
                rel_dict[rel_text] = len(rel_dict)
                rel_id_to_text_dict[rel_dict[rel_text]] = rel_text

            source = node_id_set[src_text]
            target = node_id_set[dst_text]
            rel_type = "REL_" + str(rel_dict[rel_text])
            rel_split = rel_types[file_name]

            dataset[rel_split][rel_type].append(
                {
                    "source": source,
                    "source_text": src_text,
                    "target": target,
                    "target_text": dst_text,
                    # "rel_text": rel_text,
                }
            )

    print("Number of nodes: ", len(node_id_set))
    for rel_split in dataset:
        print(
            f"Number of relationships of type {rel_split}: ",
            sum([len(dataset[rel_split][rel_type]) for rel_type in dataset[rel_split]]),
        )
    return dataset


dataset = read_data()


def put_data_in_db(dataset):
    for rel_split in tqdm(dataset, desc="Relationship"):
        for rel_type in tqdm(dataset[rel_split], mininterval=1, leave=False):
            edges = dataset[rel_split][rel_type]

            # MERGE (n)-[:{rel_type} {{text:l.rel_text}}]->(m)
            gds.run_cypher(
                f"""
                UNWIND $ll as l
                MERGE (n:Entity {{id:l.source, text:l.source_text}})
                MERGE (m:Entity {{id:l.target, text:l.target_text}})
                MERGE (n)-[:{rel_split}]->(m)
                MERGE (n)-[:{rel_type}]->(m)
                """,
                params={"ll": edges},
            )

    for rel_split in dataset:
        res = gds.run_cypher(
            f"""
            MATCH ()-[r:{rel_split}]->()
            RETURN COUNT(r) AS numberOfRelationships
            """
        )
        print(f"Number of relationships of type {rel_split} in db: ", res.numberOfRelationships)


# put_data_in_db(dataset)

ALL_RELS = dataset["TRAIN"].keys()
gds.graph.drop("trainGraph", failIfMissing=False)
G_train, result = gds.graph.cypher.project(
    """
    MATCH (n:Entity)-[:TRAIN]->(m:Entity)<-[:"""
    + "|".join(ALL_RELS)
    + """]-(n)
    RETURN gds.graph.project($graph_name, n, m, {
        sourceNodeLabels: $label,
        targetNodeLabels: $label
    })
    """,  #  Cypher query
    database="neo4j",  #  Target database
    graph_name="trainGraph",  #  Query parameter
    label="Entity",  #  Query parameter
)


def inspect_graph(G):
    func_names = [
        "name",
        # "database",
        "node_count",
        "relationship_count",
        "node_labels",
        "relationship_types",
        # "degree_distribution", "density", "size_in_bytes", "memory_usage", "exists", "configuration", "creation_time", "modification_time",
    ]
    for func_name in func_names:
        print(f"==={func_name}===: {getattr(G, func_name)()}")


inspect_graph(G_train)

gds.set_compute_cluster_ip("localhost")

kkge = gds.kge

gds.kge.model.train(
    G_train,
    scoring_function="distmult",
    num_epochs=10,
    embedding_dimension=100,
)
#
# node_projection = {"Entity": {"properties": "id"}}
# relationship_projection = [
#     {"TRAIN": {"orientation": "NATURAL", "properties": "rel_id"}},
#     {"TEST": {"orientation": "NATURAL", "properties": "rel_id"}},
#     {"VALID": {"orientation": "NATURAL", "properties": "rel_id"}},
# ]
#
# ttv_G, result = gds.graph.project(
#     "fb15k-graph-ttv",
#     node_projection,
#     relationship_projection,
# )
#
# node_properties = gds.graph.nodeProperties.stream(
#     ttv_G,
#     ["id"],
#     separate_property_columns=True,
# )
#
# nodeId_to_id = dict(zip(node_properties.nodeId, node_properties.id))
# id_to_nodeId = dict(zip(node_properties.id, node_properties.nodeId))
#
# def create_data_from_graph(relationship_type):
#     rels_tmp = gds.graph.relationshipProperty.stream(ttv_G, "rel_id", relationship_type)
#     topology = [
#         rels_tmp.sourceNodeId.map(lambda x: nodeId_to_id[x]),
#         rels_tmp.targetNodeId.map(lambda x: nodeId_to_id[x]),
#     ]
#     edge_index = torch.tensor(topology, dtype=torch.long)
#     edge_type = torch.tensor(rels_tmp.propertyValue.astype(int), dtype=torch.long)
#     data = Data(edge_index=edge_index, edge_type=edge_type)
#     data.num_nodes = len(nodeId_to_id)
#     display(data)
#     return data
#
#
# train_tensor_data = create_data_from_graph("TRAIN")
# test_tensor_data = create_data_from_graph("TEST")
# val_tensor_data = create_data_from_graph("VALID")
#
# gds.graph.drop(ttv_G)
#
# def train_model_with_pyg():
#     device = "cuda" if torch.cuda.is_available() else "cpu"
#
#     model = TransE(
#         num_nodes=train_tensor_data.num_nodes,
#         num_relations=train_tensor_data.num_edge_types,
#         hidden_channels=50,
#     ).to(device)
#
#     loader = model.loader(
#         head_index=train_tensor_data.edge_index[0],
#         rel_type=train_tensor_data.edge_type,
#         tail_index=train_tensor_data.edge_index[1],
#         batch_size=1000,
#         shuffle=True,
#     )
#
#     optimizer = optim.Adam(model.parameters(), lr=0.01)
#
#     def train():
#         model.train()
#         total_loss = total_examples = 0
#         for head_index, rel_type, tail_index in loader:
#             optimizer.zero_grad()
#             loss = model.loss(head_index, rel_type, tail_index)
#             loss.backward()
#             optimizer.step()
#             total_loss += float(loss) * head_index.numel()
#             total_examples += head_index.numel()
#         return total_loss / total_examples
#
#     @torch.no_grad()
#     def test(data):
#         model.eval()
#         return model.test(
#             head_index=data.edge_index[0],
#             rel_type=data.edge_type,
#             tail_index=data.edge_index[1],
#             batch_size=1000,
#             k=10,
#         )
#
#     # Consider increasing the number of epochs
#     epoch_count = 5
#     for epoch in range(1, epoch_count):
#         loss = train()
#         print(f"Epoch: {epoch:03d}, Loss: {loss:.4f}")
#         if epoch % 75 == 0:
#             rank, hits = test(val_tensor_data)
#             print(f"Epoch: {epoch:03d}, Val Mean Rank: {rank:.2f}, " f"Val Hits@10: {hits:.4f}")
#
#     torch.save(model, f"./model_{epoch_count}.pt")
#
#     mean_rank, mrr, hits_at_k = test(test_tensor_data)
#     print(f"Test Mean Rank: {mean_rank:.2f}, Test Hits@10: {hits_at_k:.4f}, MRR: {mrr:.4f}")
#
#     return model
#
# model = train_model_with_pyg()
# # The model can be loaded if it was trained before
# # model = torch.load("./model_501.pt")
#
# for i in tqdm(range(len(nodeId_to_id))):
#     gds.run_cypher(
#         "MATCH (n:Entity {id: $i}) SET n.emb=$EMBEDDING",
#         params={"i": i, "EMBEDDING": model.node_emb.weight[i].tolist()},
#     )
#
# relationship_to_predict = "/film/film/genre"
# rel_id_to_predict = rel_dict[relationship_to_predict]
# rel_label_to_predict = f"REL_{rel_id_to_predict}"
#
# G_test, result = gds.graph.project(
#     "graph_to_predict_",
#     {"Entity": {"properties": ["id", "emb"]}},
#     rel_label_to_predict,
# )
#
#
# def print_graph_info(G):
#     print(f"Graph '{G.name()}' node count: {G.node_count()}")
#     print(f"Graph '{G.name()}' node labels: {G.node_labels()}")
#     print(f"Graph '{G.name()}' relationship types: {G.relationship_types()}")
#     print(f"Graph '{G.name()}' relationship count: {G.relationship_count()}")
#
#
# print_graph_info(G_test)
#
# target_emb = model.node_emb.weight[rel_id_to_predict].tolist()
# transe_model = gds.model.transe.create(G_test, "emb", {rel_label_to_predict: target_emb})
#
# source_node_list = ["/m/07l450", "/m/0ds2l81", "/m/0jvt9"]
# source_ids_df = gds.run_cypher(
#     "UNWIND $node_text_list AS t MATCH (n:Entity) WHERE n.text=t RETURN id(n) as nodeId",
#     params={"node_text_list": source_node_list},
# )
#
# result = transe_model.predict_stream(
#     source_node_filter=source_ids_df.nodeId,
#     target_node_filter="Entity",
#     relationship_type=rel_label_to_predict,
#     top_k=3,
#     concurrency=4,
# )
# print(result)
#
# ids_in_result = pd.unique(pd.concat([result.sourceNodeId, result.targetNodeId]))
#
# ids_to_text = gds.run_cypher(
#     "UNWIND $ids AS id MATCH (n:Entity) WHERE id(n)=id RETURN id(n) AS nodeId, n.text AS tag, n.id AS id",
#     params={"ids": ids_in_result},
# )
#
# nodeId_to_text_res = dict(zip(ids_to_text.nodeId, ids_to_text.tag))
# nodeId_to_id_res = dict(zip(ids_to_text.nodeId, ids_to_text.id))
#
# result.insert(1, "sourceTag", result.sourceNodeId.map(lambda x: nodeId_to_text_res[x]))
# result.insert(2, "sourceId", result.sourceNodeId.map(lambda x: nodeId_to_id_res[x]))
# result.insert(4, "targetTag", result.targetNodeId.map(lambda x: nodeId_to_text_res[x]))
# result.insert(5, "targetId", result.targetNodeId.map(lambda x: nodeId_to_id_res[x]))
#
# print(result)
#
# write_relationship_type = "PREDICTED_" + rel_label_to_predict
# result_write = transe_model.predict_write(
#     source_node_filter=source_ids_df.nodeId,
#     target_node_filter="Entity",
#     relationship_type=rel_label_to_predict,
#     write_relationship_type=write_relationship_type,
#     write_property="transe_score",
#     top_k=3,
#     concurrency=4,
# )
#
# gds.run_cypher(
#     "MATCH (n)-[r:"
#     + write_relationship_type
#     + "]->(m) RETURN n.id AS sourceId, n.text AS sourceTag, m.id AS targetId, m.text AS targetTag, r.transe_score AS score"
# )
#
# gds.graph.drop(G_test)
