from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_revenue_delta(daily_sales: DataFrame) -> DataFrame:

    result_df = daily_sales.select(
        F.col("sale_date"),
        F.col("revenue"),
        (F.col("revenue") - F.lag(F.col("revenue")).over(Window.orderBy(F.col("sale_date")))).alias("revenue_delta"),
    ).orderBy(F.col("sale_date")).limit(3)

    return result_df