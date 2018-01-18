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
    read(spark, header, inferSchema, inputFormat, Seq(path): _*)
  }

  def read(spark: SparkSession,
           header: Boolean,
           inferSchema: Boolean,
           inputFormat: InputFormat.Value,
           paths: String*): Dataset[Row] = {
    val options =
      Map("header" -> header.toString, "inferSchema" -> inferSchema.toString)
    read(spark, options, inputFormat, paths: _*)
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
    if (paths == null || paths.exists(path => path.isEmpty || path == null))
      return null

    val configuration = new Configuration
    configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)

    val reader =
      if (options == null || options.isEmpty || inputFormat == InputFormat.TSV || inputFormat == InputFormat.SSV) {
        spark.read
      } else {
        spark.read.options(options)
      }

    try {
      inputFormat match {
        case InputFormat.CSV     => reader.csv(paths: _*)
        case InputFormat.PARQUET => reader.parquet(paths: _*)
        case InputFormat.TSV =>
          reader
            .options(
              options + ("sep" -> InputFormat
                .getFieldDelimiter(InputFormat.TSV)
                .toString))
            .csv(paths: _*)
        case InputFormat.SSV =>
          reader
            .options(
              options + ("sep" -> InputFormat
                .getFieldDelimiter(InputFormat.SSV)
                .toString))
            .csv(paths: _*)
        case InputFormat.JSON => reader.json(paths: _*)
        case inputType =>
          throw new UnsupportedOperationException(
            s"Input type $inputType not supported")
      }
    } catch {
      case e: Exception =>
        println(
          s"${e.getMessage} while reading the data from ${paths.toString()}")
        null
    }
  }
}
