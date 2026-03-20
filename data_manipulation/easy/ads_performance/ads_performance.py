from pyspark.sql import DataFrame, functions as F


def ads_performance(ads: DataFrame) -> DataFrame:
    clicked_df = ads.groupBy("ad_id").agg(
        F.sum(F.when(F.col("action") == "Clicked", F.lit(1)).otherwise(F.lit(0))).alias("clicked")
    )

    total_df = ads.groupBy("ad_id").agg(
        F.sum(F.when(F.col("action").isin("Clicked", "Viewed"), F.lit(1)).otherwise(F.lit(0))).alias("total")
    )

    result_df = (
        clicked_df.alias("t1")
        .join(total_df.alias("t2"), F.col("t1.ad_id") == F.col("t2.ad_id"), "inner")
        .select(
            F.col("t1.ad_id").alias("ad_id"),
            F.coalesce(
                F.round((F.col("clicked").cast("double") / F.nullif(F.col("total").cast("double"), F.lit(0))) * 100, 2),
                F.lit(0.0),
            ).alias("ctr"),
        )
        .orderBy(F.col("ctr").desc(), F.col("ad_id"))
    )

    return result_df
