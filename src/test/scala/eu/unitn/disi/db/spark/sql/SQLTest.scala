package eu.unitn.disi.db.spark.sql

import eu.unitn.disi.db.spark.io.{FSType, SparkReader}
import eu.unitn.disi.db.spark.utils.InputFormat.CSV
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.Test

class SQLTest {
  @Test
  def testQueryExecutorJava(): Unit = {
    val spark: SparkSession =
      SparkSession.builder().appName("test").master("local").getOrCreate()
    val dataset: Dataset[Row] = SparkReader.read(
      spark,
      header = true,
      inferSchema = true,
      CSV,
      "src/test/resources/sample.csv")
    dataset.createOrReplaceTempView("tableTest")
    val query = "SELECT * FROM tableTest"
    QueryExecutor.executeQuery(spark, query).show()
    QueryExecutor
      .executeQuery(spark, "src/test/resources/query.txt", FSType.FS)
      .show()
  }
}
