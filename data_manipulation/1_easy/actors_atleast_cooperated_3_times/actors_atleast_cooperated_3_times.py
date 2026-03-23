from pyspark.sql import DataFrame, functions as F


def actors_atleast_cooperated_3_times(actordirector: DataFrame) -> DataFrame:

    
        return actordirector.groupBy(
                        F.col("actor_id"),
                        F.col("director_id")
                ).agg(
                        F.count(F.lit(1)).alias("actor_cooperated_count")
                ).filter(
                        F.col("actor_cooperated_count")>=3
                ).select(
                        F.col("actor_id"),
                        F.col("director_id")
                )
    