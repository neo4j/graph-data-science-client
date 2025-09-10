from pandas import DataFrame


def transpose_property_columns(result: DataFrame, list_node_labels: bool) -> DataFrame:
    wide_result = result.pivot(index=["nodeId"], columns=["nodeProperty"], values="propertyValue")
    if list_node_labels:
        labels_df = result[["nodeId", "nodeLabels"]]
        # drop duplicates so that we only have a single row for each node id
        labels_df = labels_df.drop_duplicates(ignore_index=False, subset=["nodeId"])
        labels_df.set_index("nodeId", inplace=True)

        wide_result = wide_result.join(labels_df, on="nodeId")
    wide_result = wide_result.reset_index()
    wide_result.columns.name = None

    return wide_result
