from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_revenue_delta(daily_sales: DataFrame) -> DataFrame:
    w = Window.orderBy(F.col("sale_date"))

    return (
        daily_sales
        .select(
            F.col("sale_date"),
            F.col("revenue"),
            (F.col("revenue") - F.lag(F.col("revenue")).over(w)).alias("revenue_delta"),
        )
        .orderBy(F.col("sale_date"))
        .limit(3)
    )
