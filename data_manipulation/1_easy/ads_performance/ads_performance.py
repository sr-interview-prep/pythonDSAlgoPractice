from pyspark.sql import DataFrame, functions as F


def ads_performance(ads: DataFrame) -> DataFrame:

    result = ads.groupBy("ad_id")\
        .agg(
            F.sum(
                F.when(F.col("action")=="Clicked",1) \
                .otherwise(0)
                ).alias("clicked_count"), 
            F.sum(
                F.when(F.col("action") \
                .isin("Clicked", "Viewed"),1) \
                .otherwise(0)
                ).alias("total")
            ) \
        .withColumn(
            "ctr",
                F.when(F.col("total") == 0, F.lit(0.0)) \
                .otherwise(
                    F.round(
                        (F.col("clicked_count") / F.col("total")) * 100,
                    2))) \
        .select("ad_id", "ctr")

    return result
