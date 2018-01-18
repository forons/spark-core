package eu.unitn.disi.db.spark.io

import eu.unitn.disi.db.spark.utils.InputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.Seq

object SparkReader {

  def read(spark: SparkSession,
           header: Boolean,
           inferSchema: Boolean,
           inputFormat: InputFormat.Value,
           path: String): Dataset[Row] = {
    val options =
      Map("header" -> header.toString, "inferSchema" -> inferSchema.toString)
    read(spark, options, inputFormat, path)
  }

  def read(spark: SparkSession,
           options: Map[String, String],
           inputFormat: InputFormat.Value,
           path: String): Dataset[Row] = {
    read(spark, options, inputFormat, Seq(path): _*)
  }

  def read(spark: SparkSession,
           options: Map[String, String],
           inputFormat: InputFormat.Value,
           paths: String*): Dataset[Row] = {
    var dataset: Dataset[Row] = null
    if (paths == null || paths.exists(path => path.isEmpty || path == null))
      return dataset

    val configuration = new Configuration
    configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)

    try {
      inputFormat match {
        case InputFormat.CSV =>
          dataset = spark.read.options(options).csv(paths: _*)
        case InputFormat.PARQUET =>
          dataset = spark.read.options(options).parquet(paths: _*)
        case InputFormat.TSV =>
          dataset = spark.read
            .options(
              options + ("sep" -> InputFormat
                .getFieldDelimiter(InputFormat.TSV)
                .toString))
            .csv(paths: _*)
        case InputFormat.SSV =>
          dataset = spark.read
            .options(
              options + ("sep" -> InputFormat
                .getFieldDelimiter(InputFormat.SSV)
                .toString))
            .csv(paths: _*)
        case InputFormat.JSON =>
          dataset = spark.read.options(options).json(paths: _*)
        case _ =>
          throw new UnsupportedOperationException("Input type not supported")
      }
    } catch {
      case e: Exception =>
        println(
          e.getMessage + " while reading the data from " + paths.toString())
    }
    dataset
  }
}
