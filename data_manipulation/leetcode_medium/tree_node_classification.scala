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
  // Kept for local parity runner compatibility.
  val query: String =
    """
      |SELECT
      |  t.id,
      |  CASE
      |    WHEN t.p_id IS NULL THEN 'Root'
      |    WHEN COUNT(c.id) = 0 THEN 'Leaf'
      |    ELSE 'Inner'
      |  END AS type
      |FROM Tree t
      |LEFT JOIN Tree c
      |  ON t.id = c.p_id
      |GROUP BY t.id, t.p_id
      |ORDER BY t.id
      |""".stripMargin

  def solve(treeDf: DataFrame): DataFrame = {
    val classifier = new TreeNodeClassifier()
    classifier.classify(treeDf)
  }

  def run(spark: SparkSession): DataFrame = {
    val treeDf = spark.table("Tree")
    solve(treeDf)
  }
}
