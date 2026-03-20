from pyspark.sql import DataFrame, functions as F


def tree_node_classification(Tree: DataFrame) -> DataFrame:
    t = Tree.alias("t")
    c = Tree.alias("c")

    joined_df=t.join(c, F.col("t.id") == F.col("c.p_id"), "left")
    grouped_df=joined_df.groupBy(F.col("t.id"), F.col("t.p_id")).agg(F.count(F.col("c.id")).alias("child_count"))
    result_df=grouped_df.select(
        F.col("t.id"),
        F.when(F.col("t.p_id").isNull(), F.lit("Root"))
        .when(F.col("child_count") == 0, F.lit("Leaf"))
        .otherwise(F.lit("Inner"))
        .alias("type"),
    )
    return result_df.orderBy(F.col("t.id"))