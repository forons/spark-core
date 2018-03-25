package eu.unitn.disi.db.spark.sql

import eu.unitn.disi.db.spark.io.{Format, FSType, SparkReader}
import org.junit.Test

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SQLTest {
  @Test
  def scalaQueryExecutorTest(): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("test")
        .master("local")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    val dataset: Dataset[Row] = SparkReader.read(
      spark,
      header = true,
      inferSchema = true,
      Format.CSV,
      "src/test/resources/sample.csv")
    dataset.createOrReplaceTempView("tableTest")
    val query = "SELECT * FROM tableTest"
    QueryExecutor.executeQuery(spark, query).show()
    QueryExecutor
      .executeQuery(spark, "src/test/resources/query.txt", FSType.FS)
      .show()
  }
}
