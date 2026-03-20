import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class DailyRevenueDeltaCalculator {
  def calculate(dailySalesDf: DataFrame): DataFrame = {
    val orderedWindow = Window.orderBy(col("sale_date"))

    dailySalesDf
      .select(
        col("sale_date"),
        col("revenue"),
        (col("revenue") - lag(col("revenue"), 1).over(orderedWindow)).as("revenue_delta")
      )
      .orderBy(col("sale_date"))
      .limit(3)
  }
}

object DailyRevenueDelta {
  def solve(dailySalesDf: DataFrame): DataFrame = {
    val calculator = new DailyRevenueDeltaCalculator()
    calculator.calculate(dailySalesDf)
  }

  def run(spark: SparkSession): DataFrame = {
    val dailySalesDf = spark.table("daily_sales")
    solve(dailySalesDf)
  }
}
