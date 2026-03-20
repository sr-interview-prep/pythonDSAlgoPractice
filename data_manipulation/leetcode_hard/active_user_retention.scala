import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ActiveUserRetentionCalculator {
  def calculate(userActionsDf: DataFrame): DataFrame = {
    val monthlyActivity = userActionsDf
      .filter(col("event_type").isin("sign-in", "like", "comment"))
      .select(
        col("user_id"),
        date_trunc("month", col("event_date")).as("month")
      )
      .distinct()

    val current = monthlyActivity.alias("c")
    val previous = monthlyActivity.alias("p")

    current
      .join(
        previous,
        col("p.user_id") === col("c.user_id") &&
          col("p.month") === add_months(col("c.month"), -1),
        "inner"
      )
      .select(
        col("c.user_id").as("user_id"),
        col("c.month").as("month")
      )
      .filter(col("month") === to_date(lit("2022-07-01")))
      .groupBy(month(col("month")).as("mth"))
      .agg(countDistinct(col("user_id")).as("monthly_active_users"))
  }
}

object ActiveUserRetention {
  // Kept for local parity runner compatibility.
  val query: String =
    """
      |WITH m AS (
      |  SELECT DISTINCT
      |    user_id,
      |    DATE_TRUNC('month', event_date) AS month
      |  FROM user_actions
      |  WHERE event_type IN ('sign-in', 'like', 'comment')
      |),
      |active AS (
      |  SELECT c.user_id, c.month
      |  FROM m c
      |  JOIN m p
      |    ON p.user_id = c.user_id
      |   AND p.month = ADD_MONTHS(c.month, -1)
      |)
      |SELECT
      |  MONTH(month) AS mth,
      |  COUNT(DISTINCT user_id) AS monthly_active_users
      |FROM active
      |WHERE month = DATE '2022-07-01'
      |GROUP BY 1
      |""".stripMargin

  def solve(userActionsDf: DataFrame): DataFrame = {
    val calculator = new ActiveUserRetentionCalculator()
    calculator.calculate(userActionsDf)
  }

  def run(spark: SparkSession): DataFrame = {
    val userActionsDf = spark.table("user_actions")
    solve(userActionsDf)
  }
}
