package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.io.{Format, SparkReader, SparkWriter}
import org.junit.{After, Before, Test}

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class FilterTest {

  var spark: SparkSession = _
  var dataset: Dataset[Row] = _
  var result: Dataset[Row] = _
  val options = Map("header" -> "true", "inferSchema" -> "true")

  @Before
  def init(): Unit = {
    spark =
      SparkSession
        .builder()
        .appName("test")
        .master("local")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    dataset = SparkReader.read(
      spark,
      header = true,
      inferSchema = true,
      Format.CSV,
      "src/test/resources/sample.csv")
  }

  @After
  def stop(): Unit = {
    if (result != null) {
      result.show
      SparkWriter.write(spark, result, options, "out/out.csv", Format.CSV)
    }
  }

  @Test
  def scalaFilterListTest(): Unit = {
    val whitelist = ("city", "san francisco") :: (0, 1) :: Nil
    val blacklist = ("name", "la taqueria") :: (0, 11) :: Nil
    val colsToKeep = "0" :: "city" :: "name" :: Nil

    result = Filter.applyFilter(dataset, whitelist, blacklist, colsToKeep)
  }

  @Test
  def scalaFilterMapTest(): Unit = {
    val whitelist = Map("city" -> "san francisco", 0 -> (1 :: 2 :: Nil))
    val blacklist = ("name", "la taqueria") :: (0, 11) :: Nil
    val colsToKeep = "0" :: "city" :: "name" :: Nil

    result = Filter.applyFilter(dataset, whitelist, blacklist, colsToKeep)
  }
}
