from pyspark.sql import DataFrame, functions as F


def actors_atleast_cooperated_3_times(actordirector: DataFrame) -> DataFrame:
    return (
        actordirector
        .groupBy("actor_id", "director_id")
        .agg(F.count(F.lit(1)).alias("cooperation_count"))
        .filter(F.col("cooperation_count") >= 3)
        .select("actor_id", "director_id")
    )
