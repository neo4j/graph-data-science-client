import numpy as np
import pandas as pd

# Raw data is taken from the zip at https://grouplens.org/datasets/hetrec-2011/
# Download and put the files at some local directory and modify readlines to read from the appropriate path


def readlines(path: str) -> list[str]:
    with open(path, "r") as f:
        return list(map(lambda line: line.strip(), f.readlines()))


# There are artists in rels that are missing in artists.dat
# artist_lines = readlines("raw2k/artists.dat")
# split_artist = [line.split('\t')[:1] for line in artist_lines[1:]]
# artist_df = pd.DataFrame(split_artist, columns=['nodeId'])
# artist_df['nodeId'] = artist_df['nodeId'].astype(int)
# artist_df
# artist_df['labels'] = 'Artist'
# artist_df['nodeId'] = artist_df['nodeId'] + 10000
# artist_df.reset_index(drop=True, inplace=True)

# User LISTEN_TO Artist relationships
user_listen_artist_lines = readlines("raw/user_artists.dat")
split_user_listen_artist = [line.split("\t")[:3] for line in user_listen_artist_lines[1:]]
user_artist_df = pd.DataFrame(split_user_listen_artist, columns=["sourceNodeId", "targetNodeId", "weight"])
user_artist_df["sourceNodeId"] = user_artist_df["sourceNodeId"].astype(int)
user_artist_df["targetNodeId"] = user_artist_df["targetNodeId"].astype(int)
user_artist_df["weight"] = user_artist_df["weight"].astype(int)
# Increment artists ids to avoid clash with usrs ids
user_artist_df["targetNodeId"] = user_artist_df["targetNodeId"] + 10000
user_artist_df["relationshipType"] = "LISTEN_TO"
user_artist_df.reset_index(drop=True, inplace=True)

# User nodes
user_df = pd.DataFrame({"nodeId": user_artist_df["sourceNodeId"].drop_duplicates(), "labels": "User"})
user_df["rawId"] = user_df["nodeId"]
user_df.reset_index(drop=True, inplace=True)

# User TAG Artist relationships
user_tag_dmy = readlines("raw/user_taggedartists.dat")
split_user_tag_dmy = [line.split("\t") for line in user_tag_dmy[1:]]
# Create a pandas DataFrame with the split data
user_tag_dmy_df = pd.DataFrame(
    split_user_tag_dmy, columns=["sourceNodeId", "targetNodeId", "tagID", "day", "month", "year"]
)
user_tag_dmy_df["relationshipType"] = "TAGGED"
# UNCOMMENT TO ENCODE EACH TAGID AS A DIFFERENT RELATIONSHIP TYPE
# user_tag_dmy_df["relationshipType"] = "TAGGED_" + user_tag_dmy_df["tagID"]
user_tag_timestamp = readlines("raw/user_taggedartists-timestamps.dat")
split_user_tag_timestamp = [line.split("\t") for line in user_tag_timestamp[1:]]
# Create a pandas DataFrame with the split data
user_tag_timestamp_df = pd.DataFrame(
    split_user_tag_timestamp, columns=["sourceNodeId", "targetNodeId", "tagID", "timestamp"]
)
user_tag_timestamp_df["relationshipType"] = "TAGGED"
# UNCOMMENT TO ENCODE EACH TAGID AS A DIFFERENT RELATIONSHIP TYPE
# user_tag_timestamp_df["relationshipType"] = "TAGGED_" + user_tag_timestamp_df["tagID"]
user_tag_artist_df = user_tag_dmy_df.join(
    user_tag_timestamp_df.set_index(["sourceNodeId", "targetNodeId", "tagID", "relationshipType"]),
    on=["sourceNodeId", "targetNodeId", "tagID", "relationshipType"],
)
user_tag_artist_df["sourceNodeId"] = user_tag_artist_df["sourceNodeId"].astype(int)
user_tag_artist_df["targetNodeId"] = user_tag_artist_df["targetNodeId"].astype(int)
# Increment artists ids to avoid clash with usrs ids
user_tag_artist_df["targetNodeId"] = user_tag_artist_df["targetNodeId"] + 10000
user_tag_artist_df["tagID"] = user_tag_artist_df["tagID"].astype(int)
user_tag_artist_df["timestamp"] = user_tag_artist_df["timestamp"].astype(int)
user_tag_artist_df["day"] = user_tag_artist_df["day"].astype(int)
user_tag_artist_df["month"] = user_tag_artist_df["month"].astype(int)
user_tag_artist_df["year"] = user_tag_artist_df["year"].astype(int)
user_tag_artist_df.reset_index(drop=True, inplace=True)

# User FRIEND User relationships
user_friends = readlines("raw/user_friends.dat")
split_user_friends = [line.split("\t") for line in user_friends[1:]]
# Create a pandas DataFrame with the split data
user_friend_df = pd.DataFrame(split_user_friends, columns=["sourceNodeId", "targetNodeId"])
user_friend_sorted = np.sort(user_friend_df.values, axis=1)
sorted_user_friend_df = pd.DataFrame(user_friend_sorted, columns=["sourceNodeId", "targetNodeId"])
user_friend_df_directed = sorted_user_friend_df.drop_duplicates()
user_friend_df_directed["relationshipType"] = "IS_FRIEND"
user_friend_df_directed["sourceNodeId"] = user_friend_df_directed["sourceNodeId"].astype(int)
user_friend_df_directed["targetNodeId"] = user_friend_df_directed["targetNodeId"].astype(int)
user_friend_df_directed.reset_index(drop=True, inplace=True)

# Artists
artists_df1 = user_artist_df[["targetNodeId"]].drop_duplicates().reset_index(drop=True)
artists_df2 = user_tag_artist_df[["targetNodeId"]].drop_duplicates().reset_index(drop=True)
artist_df = (
    pd.concat([artists_df1, artists_df2])
    .drop_duplicates()
    .reset_index(drop=True)
    .rename({"targetNodeId": "nodeId"}, axis=1)
)
artist_df["rawId"] = artist_df["nodeId"]
artist_df["labels"] = "Artist"


artist_df.to_parquet(
    "artist_nodes.parquet.gzip",
    compression="gzip",
)
user_df.to_parquet(
    "user_nodes.parquet.gzip",
    compression="gzip",
)
user_artist_df.to_parquet(
    "user_listen_artist_rels.parquet.gzip",
    compression="gzip",
)
user_tag_artist_df.to_parquet(
    "user_tag_artist_rels.parquet.gzip",
    compression="gzip",
)
user_friend_df_directed.to_parquet(
    "user_friend_df_directed.parquet.gzip",
    compression="gzip",
)
