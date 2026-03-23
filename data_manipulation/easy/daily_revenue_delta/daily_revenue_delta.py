from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_revenue_delta(daily_sales: DataFrame) -> DataFrame:
    
    window_spec = Window.orderBy(F.col("sale_date"))
    result = daily_sales \
            .select(
                F.col("sale_date"),
                F.col("revenue"),
                (F.col("revenue") - F.lag(F.col("revenue")).over(window_spec))
                    .alias("revenue_delta")
            ) \
            .orderBy("sale_date") \
            .limit(3)

    return result
    