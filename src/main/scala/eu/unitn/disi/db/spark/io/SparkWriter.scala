package eu.unitn.disi.db.spark.io

import java.io.IOException
import java.net.{URI, URISyntaxException}

import eu.unitn.disi.db.spark.utils.OutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.JavaConversions

object SparkWriter {

  def write(spark: SparkSession,
            dataset: Dataset[_],
            header: Boolean,
            path: String,
            outputFormat: OutputFormat.Value): Boolean = {
    write(spark,
      dataset,
      Map("header" -> header.toString),
      path,
      outputFormat,
      null)
  }

  def write(spark: SparkSession,
            dataset: Dataset[_],
            options: java.util.Map[String, String],
            path: String,
            outputFormat: OutputFormat.Value): Boolean = {
    write(spark, dataset, JavaConversions.mapAsScalaMap(options).toMap, path, outputFormat)
  }

  def write(spark: SparkSession,
            dataset: Dataset[_],
            options: java.util.Map[String, String],
            path: String,
            outputFormat: OutputFormat.Value,
            filename: String): Boolean = {
    write(spark, dataset, JavaConversions.mapAsScalaMap(options).toMap, path, outputFormat, filename)
  }


  def write(spark: SparkSession,
            dataset: Dataset[_],
            options: Map[String, String],
            path: String,
            outputFormat: OutputFormat.Value,
            filename: String = "output"): Boolean = {
    if (dataset == null || path == null || path.isEmpty) {
      System.out.println("Not able to write the data...")
      return false
    }
    try {
      val writer = if (options != null && options.nonEmpty) {
        outputFormat match {
          case OutputFormat.SINGLE_CSV =>
            dataset.coalesce(1).write.options(options).mode(SaveMode.Overwrite)
          case _ => dataset.write.options(options).mode(SaveMode.Overwrite)
        }
      } else {
        dataset.write.mode(SaveMode.Overwrite)
      }

      outputFormat match {
        case OutputFormat.CSV =>
          writer.csv(path)
          return true
        case OutputFormat.PARQUET =>
          writer.save(path)
          return true
        case OutputFormat.SINGLE_CSV =>
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
          } catch {
            case e@(_: IllegalArgumentException | _: IOException |
                    _: URISyntaxException) =>
              println("Error " + e.getMessage + "while writing the data!")
              e.printStackTrace()
              return false
          }
          return true
        case OutputFormat.JSON =>
          writer.json(path)
          return true;
        case _ =>
          return false
      }
    } catch {
      case e: UnsupportedOperationException =>
        printf("%s while writing the data to %s/%s in %s format.\n",
          e.getMessage,
          path,
          filename,
          outputFormat.toString)
    }
    false
  }
}
