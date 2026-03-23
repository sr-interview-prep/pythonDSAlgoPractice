import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TreeNodeClassifier {
  def classify(treeDf: DataFrame): DataFrame = {
    val parent = treeDf.alias("t")
    val child = treeDf.alias("c")

    parent
      .join(child, col("t.id") === col("c.p_id"), "left")
      .groupBy(col("t.id"), col("t.p_id"))
      .agg(count(col("c.id")).as("child_count"))
      .select(
        col("id"),
        when(col("p_id").isNull, lit("Root"))
          .when(col("child_count") === 0, lit("Leaf"))
          .otherwise(lit("Inner"))
          .as("type")
      )
      .orderBy(col("id"))
  }
}

object TreeNodeClassification {
  def solve(treeDf: DataFrame): DataFrame = {
    val classifier = new TreeNodeClassifier()
    classifier.classify(treeDf)
  }

  def run(spark: SparkSession): DataFrame = {
    val treeDf = spark.table("Tree")
    solve(treeDf)
  }
}
