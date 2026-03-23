from pyspark.sql import DataFrame, functions as F


def tree_node_classification(Tree: DataFrame) -> DataFrame:
    t=Tree.alias("t")
    c=Tree.alias("c")

    joined_df=t.join(
                c, 
                F.col("t.id")==F.col("c.p_id"),
                "left"
            ).groupBy(
                F.col("t.id"),
                F.col("t.p_id")
            ).agg(
                F.count(F.col("c.id")).alias("count_children")
            )\
            .select(
                F.col("id"),
                F.when(
                        F.col("p_id").isNull(),
                        F.lit("Root")
                    ).when(
                        F.col("count_children") == 0,
                        F.lit("Leaf")
                    ).otherwise(
                        F.lit("Inner")
                ).alias("type")
                )\
            .orderBy(
                F.col("id")
            )
    return joined_df
    