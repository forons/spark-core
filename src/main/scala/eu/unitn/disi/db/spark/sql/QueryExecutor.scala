package eu.unitn.disi.db.spark.sql

import java.io._
import java.net.URI

import it.unitn.dbtrento.spark.utils.FileSystemType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

class QueryExecutor {

  def executeQuery(spark: SparkSession,
                   path: String,
                   queryPath: String,
                   fsType: FileSystemType): Dataset[Row] = {
    var dataset: Dataset[Row] = null
    try {
      dataset = spark.sql(readQuery(path.concat("/").concat(queryPath), fsType))
    } catch {
      case e: Exception =>
        println("Exception " + e.getMessage + " in query " + queryPath)
    }
    dataset
  }

  def executeQueries(spark: SparkSession,
                     queries: List[(String, String)]): Seq[Dataset[Row]] = {
    queries.map(tup => spark.sql(tup._2))
  }

  def readQueries(path: String,
                  fsType: FileSystemType): Seq[(String, String)] = {
    // TODO change according to the FS type
    // Now only the FS is supported
    val folder = new File(path)
    var queries = Seq[(String, String)]()
    if (folder.isFile) {
      queries :+= (folder.getName, readQuery(folder.getAbsolutePath, fsType))
    } else if (folder.isDirectory) {
      for (file <- if (folder.listFiles() != null) folder.listFiles()
           else new Array[File](0)) {
        if (file.isFile) {
          queries :+= (file.getName, readQuery(file.getAbsolutePath, fsType))
        }
      }
    }
    queries
  }

  def readQuery(path: String, fsType: FileSystemType): String = {
    var query: String = null
    fsType match {
      case FileSystemType.FS   => query = readQueryFromFS(path)
      case FileSystemType.HDFS => query = readQueryFromHDFS(path)
      case _ =>
        throw new UnsupportedOperationException(
          "FS type " + fsType + " not supported")
    }
    query
  }

  def readQueryFromFS(path: String): String = {
    var query: String = null
    try {
      query = Source.fromFile(path).getLines().mkString("\n")
    } catch {
      case _: FileNotFoundException => println("File " + path + " not found!")
      case _: IOException           => println("File " + path + " thrown an IOException!")
    }
    query
  }

  def readQueryFromHDFS(path: String): String = {
    var query: String = null
    try {
      val fs = FileSystem.get(new URI(path), new Configuration)
      val bf = new BufferedReader(
        new InputStreamReader(fs.open(new Path(path))))
      query = Stream
        .cons(bf.readLine(), Stream.continually(bf.readLine()))
        .takeWhile(_ != null)
        .mkString("\n")
    } catch {
      case _: FileNotFoundException => println("File " + path + " not found!")
      case _: IOException           => println("File " + path + " thrown an IOException!")
    }
    query
  }
}
