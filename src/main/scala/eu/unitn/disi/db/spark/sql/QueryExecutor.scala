package eu.unitn.disi.db.spark.sql

import java.io._
import java.net.URI

import eu.unitn.disi.db.spark.io.FSType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.slf4j.{Logger, LoggerFactory}
import scala.io.Source

import org.apache.spark.sql.{Dataset, Row, SparkSession}


object QueryExecutor {

  val log : Logger = LoggerFactory.getLogger(this.getClass.getName)

  def executeQuery(spark: SparkSession,
                   path: String,
                   queryPath: String,
                   fsType: FSType): Dataset[Row] = {
    executeQuery(spark, path.concat("/").concat(queryPath), fsType)
  }

  def executeQuery(spark: SparkSession,
                   path: String,
                   fsType: FSType): Dataset[Row] = {
    executeQuery(spark, readQuery(path, fsType))
  }

  def executeQuery(spark: SparkSession, query: String): Dataset[Row] = {
    try {
      spark.sql(query)
    } catch {
      case e: Exception =>
        log.error(s"Exception ${e.getMessage} in query  $query")
        null
    }
  }

  def executeQueries(spark: SparkSession,
                     queries: List[(String, String)]): Seq[Dataset[Row]] = {
    queries.map(tup => executeQuery(spark, tup._2))
  }

  def readQueries(path: String, fsType: FSType): Seq[(String, String)] = {
    fsType match {
      case FSType.FS =>
        val folder = new File(path)
        if (folder.isFile) {
          Seq((folder.getName, readQuery(folder.getAbsolutePath, fsType)))
        } else if (folder.isDirectory) {
          folder
            .listFiles()
            .map(file =>
              (file.getName, readQuery(file.getAbsolutePath, fsType)))
            .toSeq
        } else {
          Seq[(String, String)]()
        }
      case FSType.HDFS =>
        val conf = new Configuration
        conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
        conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
        FileSystem
          .get(new URI(path), conf)
          .listStatus(new Path(path))
          .map(status =>
            (status.getPath.getName, readQuery(status.toString, fsType)))
          .toSeq
      case _ =>
        throw new UnsupportedOperationException(
          s"FS type $fsType not supported")
    }
  }

  def readQuery(path: String, fsType: FSType): String =
    fsType match {
      case FSType.FS => readQueryFromFS(path)
      case FSType.HDFS => readQueryFromHDFS(path)
      case _ =>
        throw new UnsupportedOperationException(
          s"FS type $fsType not supported")
    }

  def readQueryFromFS(path: String): String = {
    try {
      Source.fromFile(path).getLines().mkString("\n")
    } catch {
      case _: FileNotFoundException =>
        log.error(s"File $path not found!")
        null
      case _: IOException =>
        log.error(s"File $path thrown an IOException!")
        null
    }
  }

  def readQueryFromHDFS(path: String): String = {
    try {
      val fs = FileSystem.get(new URI(path), new Configuration)
      val bf = new BufferedReader(
        new InputStreamReader(fs.open(new Path(path))))
      Stream
        .cons(bf.readLine(), Stream.continually(bf.readLine()))
        .takeWhile(_ != null)
        .mkString("\n")
    } catch {
      case _: FileNotFoundException =>
        log.error(s"File $path not found!")
        null
      case _: IOException =>
        log.error(s"File $path thrown an IOException!")
        null
    }
  }
}
