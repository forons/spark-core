package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.io.{Format, SparkReader, SparkWriter}
import org.junit.Test

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class FilterTest {

  @Test
  def scalaFilterListTest(): Unit = {
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
    val whitelist = ("city", "san francisco") :: (0, 1) :: Nil
    val blacklist = ("name", "la taqueria") :: (0, 11) :: Nil
    val colsToKeep = "0" :: "city" :: "name" :: Nil

    val result = Filter.applyFilter(dataset, whitelist, blacklist, colsToKeep)
    result.show()

    val options = Map("header" -> "true", "inferSchema" -> "true")

    SparkWriter.write(spark, result, options, "out/out.csv", Format.CSV)
  }

  @Test
  def scalaFilterMapTest(): Unit = {
    val spark: SparkSession =
      SparkSession.builder().appName("test").master("local").getOrCreate()
    val dataset: Dataset[Row] = SparkReader.read(
      spark,
      header = true,
      inferSchema = true,
      Format.CSV,
      "src/test/resources/sample.csv")
    val whitelist = Map("city" -> "san francisco", 0 -> (1 :: 2 :: Nil))
    val blacklist = ("name", "la taqueria") :: (0, 11) :: Nil
    val colsToKeep = "0" :: "city" :: "name" :: Nil

    val result = Filter.applyFilter(dataset, whitelist, blacklist, colsToKeep)
    result.show()

    val options = Map("header" -> "true", "inferSchema" -> "true")

    SparkWriter.write(spark, result, options, "out/out.csv", Format.CSV)
  }
}
