package eu.unitn.disi.db.spark.io

import eu.unitn.disi.db.spark.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.Seq

import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession}

object SparkReader {

  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def read(spark: SparkSession,
           header: Boolean,
           inferSchema: Boolean,
           inputFormat: Format,
           path: String): Dataset[Row] = {
    read(spark, header, inferSchema, inputFormat, Seq(path): _*)
  }

  def read(spark: SparkSession,
           header: Boolean,
           inferSchema: Boolean,
           inputFormat: Format,
           paths: String*): Dataset[Row] = {
    val options =
      Map("header" -> header.toString, "inferSchema" -> inferSchema.toString)
    read(spark, options, inputFormat, paths: _*)
  }

  def read(spark: SparkSession,
           options: java.util.Map[String, String],
           path: String): Dataset[Row] = {
    read(spark,
         Utils.javaToScalaMap(options),
         fromExtension(path),
         Seq(path): _*)
  }

  def read(spark: SparkSession,
           options: Map[String, String],
           path: String): Dataset[Row] = {
    read(spark, options, fromExtension(path), Seq(path): _*)
  }

  def read(spark: SparkSession,
           options: java.util.Map[String, String],
           paths: String*): Dataset[Row] = {
    read(spark,
         Utils.javaToScalaMap(options),
         fromExtension(paths: _*),
         paths: _*)
  }

  def read(spark: SparkSession,
           options: Map[String, String],
           paths: String*): Dataset[Row] = {
    read(spark, options, fromExtension(paths: _*), paths: _*)
  }

  def read(spark: SparkSession,
           options: java.util.Map[String, String],
           inputFormat: Format,
           path: String): Dataset[Row] = {
    read(spark, Utils.javaToScalaMap(options), inputFormat, Seq(path): _*)
  }

  def read(spark: SparkSession,
           options: Map[String, String],
           inputFormat: Format,
           path: String): Dataset[Row] = {
    read(spark, options, inputFormat, Seq(path): _*)
  }

  def read(spark: SparkSession,
           options: java.util.Map[String, String],
           format: Format,
           paths: String*): Dataset[Row] = {
    read(spark, Utils.javaToScalaMap(options), format, paths: _*)
  }

  def read(spark: SparkSession,
           options: Map[String, String],
           format: Format,
           paths: String*): Dataset[Row] = {
    if (paths == null || paths.exists(path => path.isEmpty || path == null)) {
      log.debug(s"Error while parsing the path ${paths.toString()}")
      return null
    }

    val configuration = new Configuration
    configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)

    val reader: DataFrameReader =
      spark.read.options(Utils.addSeparatorToOptions(options, format))

    try {
      format match {
        case Format.CSV | Format.SINGLE_CSV | Format.TSV | Format.SSV =>
          reader.csv(paths: _*)
        case Format.PARQUET => reader.parquet(paths: _*)
        case Format.JSON    => reader.json(paths: _*)
        case inputType =>
          log.error(s"Input type $inputType not supported")
          throw new UnsupportedOperationException(
            s"Input type $inputType not supported")
      }
    } catch {
      case e: Exception =>
        log.debug(
          s"${e.getMessage} while reading the data from ${paths.toString()}")
        null
    }
  }

  def fromExtension(format: String): Format = {
    format
      .substring(format.lastIndexOf("."))
      .replace(".", "")
      .trim()
      .toUpperCase() match {
      case "CSV"     => Format.CSV
      case "TSV"     => Format.TSV
      case "SSV"     => Format.SSV
      case "PARQUET" => Format.PARQUET
      case "JSON"    => Format.JSON
      case inputType =>
        log.error(s"Input type $inputType not supported")
        throw new UnsupportedOperationException(
          s"Input type $inputType not supported")
    }
  }

  def fromExtension(formats: String*): Format = {
    val values = formats.map(
      format =>
        format
          .substring(format.lastIndexOf("."))
          .trim()
          .replace(".", "")
          .toUpperCase)
    if (values.distinct.size == 1) {
      fromExtension(values.head)
    } else {
      log.error(s"Multiple extensions given as input, which is not supported")
      throw new UnsupportedOperationException(
        s"Multiple extensions given as input, which is not supported")
    }
  }
}
