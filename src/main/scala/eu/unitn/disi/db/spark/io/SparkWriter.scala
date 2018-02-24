package eu.unitn.disi.db.spark.io

import java.net.URI

import eu.unitn.disi.db.spark.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}

object SparkWriter {

  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def write(spark: SparkSession,
            dataset: Dataset[_],
            header: Boolean,
            path: String,
            outputFormat: Format): Boolean = {
    write(spark, dataset, Map("header" -> header.toString), path, outputFormat)
  }

  def write(spark: SparkSession,
            dataset: Dataset[_],
            options: java.util.Map[String, String],
            path: String,
            outputFormat: Format): Boolean = {
    write(spark,
      dataset,
      Utils.javaToScalaMap[String, String](options),
      path,
      outputFormat)
  }

  def write(spark: SparkSession,
            dataset: Dataset[_],
            options: java.util.Map[String, String],
            path: String,
            outputFormat: Format,
            filename: String): Boolean = {
    write(spark,
      dataset,
      Utils.javaToScalaMap[String, String](options),
      path,
      outputFormat,
      filename)
  }

  def write(spark: SparkSession,
            dataset: Dataset[_],
            options: Map[String, String],
            path: String,
            format: Format,
            filename: String = "output"): Boolean = {
    if (dataset == null || path == null || path.isEmpty) {
      log.debug("Not able to write the data...")
      return false
    }

    val writer: DataFrameWriter[_] = (format match {
      case Format.SINGLE_CSV => dataset.coalesce(1).write
      case _ => dataset.write
    }).options(Utils.addSeparatorToOptions(options, format))
      .mode(SaveMode.Overwrite)

    var returnValue: Boolean = false

    try {
      returnValue = format match {
        case Format.CSV | Format.SSV | Format.TSV =>
          writer.csv(path)
          true
        case Format.PARQUET =>
          writer.parquet(path)
          true
        case Format.SINGLE_CSV =>
          writer.csv(path + "/partial/")
          try {
            val fullPath: String = if (filename.isEmpty || filename == null) {
              path
            } else {
              path + "/" + filename + ".csv"
            }

            FileUtil.copyMerge(
              FileSystem.get(new URI(path + "/partial/"), new Configuration),
              new Path(path + "/partial/"),
              FileSystem.get(new URI(fullPath), new Configuration),
              new Path(fullPath),
              true,
              spark.sparkContext.hadoopConfiguration,
              null
            )
            true
          } catch {
            case e: Throwable =>
              log.debug(s"Error ${e.getMessage} while writing the data!")
              false
          }
        case Format.JSON =>
          writer.json(path)
          true
        case _ => false
      }
    } catch {
      case e: UnsupportedOperationException =>
        log.error(
          s"Error ${e.getMessage} while writing the data to $path/$filename in ${format.toString} format.")
    }
    returnValue
  }
}
