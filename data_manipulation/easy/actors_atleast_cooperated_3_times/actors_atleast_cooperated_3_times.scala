import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ActorsAtleastCooperated3TimesCalculator {
  def calculate(actorDirectorDf: DataFrame): DataFrame = {
    actorDirectorDf
      .groupBy(col("actor_id"), col("director_id"))
      .agg(count(lit(1)).as("cooperation_count"))
      .filter(col("cooperation_count") >= 3)
      .select(col("actor_id"), col("director_id"))
  }
}

object ActorsAtleastCooperated3Times {
  def solve(actorDirectorDf: DataFrame): DataFrame = {
    val calculator = new ActorsAtleastCooperated3TimesCalculator()
    calculator.calculate(actorDirectorDf)
  }

  def run(spark: SparkSession): DataFrame = {
    val actorDirectorDf = spark.table("actordirector")
    solve(actorDirectorDf)
  }
}
