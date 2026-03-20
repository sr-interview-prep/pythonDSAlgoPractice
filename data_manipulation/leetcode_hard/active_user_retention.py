from pyspark.sql import SparkSession, functions as F

def active_user_retention(user_actions_df):
    # m CTE: monthly activity for selected engagement event types.
    m_df = (
        user_actions_df
        .filter(F.col("event_type").isin("sign-in", "like", "comment"))
        .select(
            F.col("user_id"),
            F.date_trunc("month", F.col("event_date")).alias("month"),
        )
        .distinct()
    )

    # active CTE: users active in both current and previous month.
    c = m_df.alias("c")
    p = m_df.alias("p")
    active_df = c.join(
        p,
        (F.col("p.user_id") == F.col("c.user_id"))
        & (F.col("p.month") == F.add_months(F.col("c.month"), -1)),
        "inner",
    ).select(
        F.col("c.user_id").alias("user_id"),
        F.col("c.month").alias("month"),
    )

    result_df = (
        active_df
        .filter(F.col("month") == F.to_date(F.lit("2022-07-01")))
        .groupBy(F.month(F.col("month")).alias("mth"))
        .agg(F.countDistinct("user_id").alias("monthly_active_users"))
    )
    return result_df

