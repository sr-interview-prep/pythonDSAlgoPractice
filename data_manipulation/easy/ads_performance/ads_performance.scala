import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class AdsPerformanceCalculator {
  def calculate(adsDf: DataFrame): DataFrame = {
    val clickedDf = adsDf
      .groupBy("ad_id")
      .agg(sum(when(col("action") === "Clicked", lit(1)).otherwise(lit(0))).as("clicked"))

    val totalDf = adsDf
      .groupBy("ad_id")
      .agg(sum(when(col("action").isin("Clicked", "Viewed"), lit(1)).otherwise(lit(0))).as("total"))

    clickedDf
      .alias("t1")
      .join(totalDf.alias("t2"), col("t1.ad_id") === col("t2.ad_id"), "inner")
      .select(
        col("t1.ad_id").as("ad_id"),
        coalesce(
          round((col("clicked").cast("double") / nullif(col("total").cast("double"), lit(0.0))) * 100, 2),
          lit(0.0)
        ).as("ctr")
      )
      .orderBy(col("ctr").desc, col("ad_id"))
  }
}

object AdsPerformance {
  def solve(adsDf: DataFrame): DataFrame = {
    val calculator = new AdsPerformanceCalculator()
    calculator.calculate(adsDf)
  }

  def run(spark: SparkSession): DataFrame = {
    val adsDf = spark.table("ads")
    solve(adsDf)
  }
}
