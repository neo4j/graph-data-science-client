import os
import time
import warnings
from collections import defaultdict

from neo4j.exceptions import ClientError
from tqdm import tqdm

from graphdatascience import GraphDataScience

warnings.filterwarnings("ignore", category=DeprecationWarning)


def setup_connection():
    NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_AUTH = None
    NEO4J_DB = os.environ.get("NEO4J_DB", "neo4j")
    if os.environ.get("NEO4J_USER") and os.environ.get("NEO4J_PASSWORD"):
        NEO4J_AUTH = (
            os.environ.get("NEO4J_USER"),
            os.environ.get("NEO4J_PASSWORD"),
        )
    gds = GraphDataScience(NEO4J_URI, auth=NEO4J_AUTH, database=NEO4J_DB, arrow=True)

    return gds


def create_constraint(gds):
    try:
        _ = gds.run_cypher("CREATE CONSTRAINT entity_id FOR (e:Entity) REQUIRE e.id IS UNIQUE")
    except ClientError:
        print("CONSTRAINT entity_id already exists")


def download_data(raw_file_names):
    import os
    import zipfile

    from ogb.utils.url import download_url

    url = "https://download.microsoft.com/download/8/7/0/8700516A-AB3D-4850-B4BB-805C515AECE1/FB15K-237.2.zip"
    raw_dir = "./data_from_zip"
    download_url(f"{url}", raw_dir)

    with zipfile.ZipFile(raw_dir + "/" + os.path.basename(url), "r") as zip_ref:
        for filename in raw_file_names:
            zip_ref.extract(f"Release/{filename}", path=raw_dir)
    data_dir = raw_dir + "/" + "Release"
    return data_dir


def get_text_to_id_map(data_dir, text_to_id_filename):
    with open(data_dir + "/" + text_to_id_filename, "r") as f:
        data = [x.split("\t") for x in f.read().split("\n")[:-1]]
    text_to_id_map = {text: int(id) for text, id in data}
    return text_to_id_map


def read_data():
    rel_types = {
        "train.txt": "TRAIN",
        "valid.txt": "VALID",
        "test.txt": "TEST",
    }
    raw_file_names = ["train.txt", "valid.txt", "test.txt"]
    node_id_filename = "entity2id.txt"
    rel_id_filename = "relation2id.txt"

    data_dir = "/Users/olgarazvenskaia/work/datasets/KGDatasets/Nations"
    node_map = get_text_to_id_map(data_dir, node_id_filename)
    rel_map = get_text_to_id_map(data_dir, rel_id_filename)
    dataset = defaultdict(lambda: defaultdict(list))

    rel_split_id = {"TRAIN": 0, "VALID": 1, "TEST": 2}

    for file_name in raw_file_names:
        file_name_path = data_dir + "/" + file_name

        with open(file_name_path, "r") as f:
            data = [x.split("\t") for x in f.read().split("\n")[:-1]]

        for i, (src_text, rel_text, dst_text) in enumerate(data):
            source = node_map[src_text]
            target = node_map[dst_text]
            rel_type = "REL_" + rel_text.upper()
            rel_split = rel_types[file_name]

            dataset[rel_split][rel_type].append(
                {
                    "source": source,
                    "source_text": src_text,
                    "target": target,
                    "target_text": dst_text,
                    "rel_type": rel_type,
                    "rel_id": rel_map[rel_text],
                    "rel_split": rel_split,
                    "rel_split_id": rel_split_id[rel_split],
                }
            )

    print("Number of nodes: ", len(node_map))
    for rel_split in dataset:
        print(
            f"Number of relationships of type {rel_split}: ",
            sum([len(dataset[rel_split][rel_type]) for rel_type in dataset[rel_split]]),
        )
    return dataset


def put_data_in_db(gds):
    res = gds.run_cypher("MATCH (m) RETURN count(m) as num_nodes")
    if res["num_nodes"].values[0] > 0:
        print("Data already in db, number of nodes: ", res["num_nodes"].values[0])
        return
    dataset = read_data()
    pbar = tqdm(
        desc="Putting data in db",
        total=sum([len(dataset[rel_split][rel_type]) for rel_split in dataset for rel_type in dataset[rel_split]]),
    )

    for rel_split in dataset:
        for rel_type in dataset[rel_split]:
            edges = dataset[rel_split][rel_type]

            gds.run_cypher(
                f"""
                UNWIND $ll as l
                MERGE (n:Entity {{id:l.source, text:l.source_text}})
                MERGE (m:Entity {{id:l.target, text:l.target_text}})
                MERGE (n)-[:{rel_type} {{split: l.rel_split_id, rel_id: l.rel_id}}]->(m)
                """,
                params={"ll": edges},
            )
            pbar.update(len(edges))
    pbar.close()

    for rel_split in dataset:
        res = gds.run_cypher(
            f"""
            MATCH ()-[r:{rel_split}]->()
            RETURN COUNT(r) AS numberOfRelationships
            """
        )
        print(f"Number of relationships of type {rel_split} in db: ", res.numberOfRelationships)


def project_graphs(gds):
    all_rels = gds.run_cypher(
        """
    CALL db.relationshipTypes() YIELD relationshipType
    """
    )
    all_rels = all_rels["relationshipType"].to_list()
    all_rels = {rel: {"properties": "split"} for rel in all_rels if rel.startswith("REL_")}
    gds.graph.drop("fullGraph", failIfMissing=False)
    gds.graph.drop("trainGraph", failIfMissing=False)
    gds.graph.drop("validGraph", failIfMissing=False)
    gds.graph.drop("testGraph", failIfMissing=False)

    G_full, _ = gds.graph.project("fullGraph", ["Entity"], all_rels)

    G_train, _ = gds.graph.filter("trainGraph", G_full, "*", "r.split = 0.0")
    G_valid, _ = gds.graph.filter("validGraph", G_full, "*", "r.split = 1.0")
    G_test, _ = gds.graph.filter("testGraph", G_full, "*", "r.split = 2.0")

    gds.graph.drop("fullGraph", failIfMissing=False)

    return G_train, G_valid, G_test


def inspect_graph(G):
    func_names = [
        "name",
        "node_count",
        "relationship_count",
        "node_labels",
        "relationship_types",
    ]
    for func_name in func_names:
        print(f"==={func_name}===: {getattr(G, func_name)()}")


if __name__ == "__main__":
    gds = setup_connection()
    create_constraint(gds)
    put_data_in_db(gds)
    G_train, G_valid, G_test = project_graphs(gds)

    inspect_graph(G_train)

    gds.set_compute_cluster_ip("localhost")

    model_name = "dummyModelName_" + str(time.time())

    res = gds.kge.model.train(
        G_train,
        model_name=model_name,
        scoring_function="TransE",
        num_epochs=30,
        embedding_dimension=64,
        epochs_per_checkpoint=0,
        epochs_per_val=0,
        split_ratios={"TRAIN": 0.8, "VALID": 0.1, "TEST": 0.1},
    )
    print(res["metrics"])

    predict_result = gds.kge.model.predict(
        model_name=model_name,
        top_k=3,
        node_ids=[
            gds.find_node_id(["Entity"], {"text": "brazil"}),
            gds.find_node_id(["Entity"], {"text": "uk"}),
            gds.find_node_id(["Entity"], {"text": "jordan"}),
        ],
        rel_types=["REL_RELDIPLOMACY", "REL_RELNGO"],
    )

    print(predict_result.to_string())

    for index, row in predict_result.iterrows():
        h = row["sourceNodeId"]
        r = row["rel"]
        gds.run_cypher(
            f"""
            UNWIND $tt as t
            MATCH (a:Entity WHERE id(a) = {h})
            MATCH (b:Entity WHERE id(b) = t)
            MERGE (a)-[:NEW_REL_{r}]->(b)
        """,
            params={"tt": row["targetNodeIdTopK"]},
        )

    brazil_node = gds.find_node_id(["Entity"], {"text": "brazil"})
    uk_node = gds.find_node_id(["Entity"], {"text": "uk"})
    jordan_node = gds.find_node_id(["Entity"], {"text": "jordan"})

    triplets = [
        (brazil_node, "REL_RELNGO", uk_node),
        (brazil_node, "REL_RELDIPLOMACY", jordan_node),
    ]

    scores = gds.kge.model.score_triplets(
        model_name=model_name,
        triplets=triplets,
    )

    print(scores)
    #
    # gds.kge.model.predict_tail(
    #     G_train,
    #     model_name=model_name,
    #     top_k=10,
    #     node_ids=[gds.find_node_id(["Entity"], {"text": "/m/016wzw"}), gds.find_node_id(["Entity"], {"id": 2})],
    #     rel_types=["REL_1", "REL_2"],
    # )
    #
    # gds.kge.model.score_triples(
    #     G_train,
    #     model_name=model_name,
    #     triples=[
    #         (gds.find_node_id(["Entity"], {"text": "/m/016wzw"}), "REL_1", gds.find_node_id(["Entity"], {"id": 2})),
    #         (gds.find_node_id(["Entity"], {"id": 0}), "REL_123", gds.find_node_id(["Entity"], {"id": 3})),
    #     ],
    # )
