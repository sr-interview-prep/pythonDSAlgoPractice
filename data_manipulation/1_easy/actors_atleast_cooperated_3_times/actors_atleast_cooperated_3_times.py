from pyspark.sql import DataFrame, functions as F


def actors_atleast_cooperated_3_times(actordirector: DataFrame) -> DataFrame:

    
    result = actordirector.groupBy("actor_id","director_id") \
            .agg(F.count(F.lit(1)).alias("cooperated_count") ) \
            .filter(F.col("cooperated_count")>=3)\
            .select("actor_id","director_id")
    return result
    
    