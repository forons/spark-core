package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.io.{SparkReader, SparkWriter}
import eu.unitn.disi.db.spark.utils.InputFormat.CSV
import eu.unitn.disi.db.spark.utils.OutputFormat
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.Test

class FilterTest {

  @Test
  def filterTestList(): Unit = {
    val spark: SparkSession =
      SparkSession.builder().appName("test").master("local").getOrCreate()
    val dataset: Dataset[Row] = SparkReader.read(
      spark,
      header = true,
      inferSchema = true,
      CSV,
      "src/test/resources/sample.csv")
    val whitelist = ("city", "san francisco") :: (0, 1) :: Nil
    val blacklist = ("name", "la taqueria") :: (0, 11) :: Nil
    val colsToKeep = "0" :: "city" :: "name" :: Nil

    val result = Filter.applyFilter(dataset, whitelist, blacklist, colsToKeep)
    result.show()

    val options = Map("header" -> "true", "inferSchema" -> "true")

    SparkWriter.write(spark, result, options, "out/out.csv", OutputFormat.CSV)
  }

  @Test
  def filterTestMap(): Unit = {
    val spark: SparkSession =
      SparkSession.builder().appName("test").master("local").getOrCreate()
    val dataset: Dataset[Row] = SparkReader.read(
      spark,
      header = true,
      inferSchema = true,
      CSV,
      "src/test/resources/sample.csv")
    val whitelist = Map("city" -> "san francisco", 0 -> (1 :: 2 :: Nil))
    val blacklist = ("name", "la taqueria") :: (0, 11) :: Nil
    val colsToKeep = "0" :: "city" :: "name" :: Nil

    val result = Filter.applyFilter(dataset, whitelist, blacklist, colsToKeep)
    result.show()

    val options = Map("header" -> "true", "inferSchema" -> "true")

    SparkWriter.write(spark, result, options, "out/out.csv", OutputFormat.CSV)
  }
}
