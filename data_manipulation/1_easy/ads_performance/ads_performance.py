from pyspark.sql import DataFrame, functions as F


def ads_performance(ads: DataFrame) -> DataFrame:

# sum of clicked
# sum of clicked and viewed
# ratio for cte

    return ads.groupBy(
            F.col("ad_id")
        ).agg(
            F.sum(F.when(F.col("action")=="Clicked",1).otherwise(0)).alias("sum_clicked"),
            F.sum(F.when(F.col("action").isin("Clicked","Viewed"),1).otherwise(0)).alias("sum_viewed_clicked")
            ) \
        .select(
            F.col("ad_id"),
            F.round(
                F.when(F.col("sum_viewed_clicked")==0,0) \
                .otherwise(F.col("sum_clicked")*100/F.col("sum_viewed_clicked")),
                2
                ).alias("ctr")) \
        .orderBy(
            F.col("ctr").desc(),
            F.col("ad_id")
        )
