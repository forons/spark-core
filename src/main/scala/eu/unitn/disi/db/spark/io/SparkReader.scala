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
           options: Map[String, String],
           inputFormat: Format,
           path: String): Dataset[Row] = {
    read(spark, options, inputFormat, Seq(path): _*)
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

    val reader: DataFrameReader = spark.read.options(Utils.addSeparatorToOptions(options, format))

    try {
      format match {
        case Format.CSV | Format.SINGLE_CSV | Format.TSV => reader.csv(paths: _*)
        case Format.PARQUET => reader.parquet(paths: _*)
        case Format.JSON => reader.json(paths: _*)
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
}
