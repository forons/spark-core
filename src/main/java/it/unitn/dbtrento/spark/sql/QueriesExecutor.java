package it.unitn.dbtrento.spark.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.unitn.dbtrento.spark.utils.FileSystemType;
import scala.Tuple2;

public class QueriesExecutor {

  public static Dataset<Row> executeQuery(SparkSession spark, String basePath, String queryFilePath,
      FileSystemType fs) throws IOException, URISyntaxException {
    Tuple2<String, String> tup = new Tuple2<>(new File(queryFilePath).getName(),
        readQuery(basePath + "/" + queryFilePath, fs));
    Dataset<Row> result = null;
    try {
      result = spark.sql(tup._2);
    } catch (Exception e) {
      System.err.println("======================================");
      System.err.println("Exception " + e.getMessage() + " in query " + tup._1);
      System.err.println("======================================");
      System.err.println("======================================");
      System.err.println(tup._2);
      System.err.println("======================================");
      System.err.println("======================================");
      e.printStackTrace();
    }
    return result;
  }

  public static List<Dataset<Row>> executeQueries(SparkSession spark, String basePath,
      String queriesFilePath, FileSystemType fs) throws IOException, URISyntaxException {
    List<Tuple2<String, String>> queries = readQueries(basePath + "/" + queriesFilePath, fs);
    List<Dataset<Row>> resultList = new ArrayList<>();
    Dataset<Row> result;
    for (Tuple2<String, String> tup : queries) {
      result = spark.sql(tup._2);
      resultList.add(result);
    }
    return resultList;
  }

  public static List<Dataset<Row>> executeQueries(SparkSession spark,
      List<Tuple2<String, String>> queries) {
    List<Dataset<Row>> resultList = new ArrayList<>();
    Dataset<Row> result;
    for (Tuple2<String, String> tup : queries) {
      result = spark.sql(tup._2);
      resultList.add(result);
    }
    return resultList;
  }

  private static List<Tuple2<String, String>> readQueries(String queriesFilePath, FileSystemType fs)
      throws IOException {
    File folder = new File(queriesFilePath);
    List<Tuple2<String, String>> list = new ArrayList<>();
    if (folder.isDirectory()) {
      File[] listOfFiles = folder.listFiles();
      System.out.println(folder.getAbsolutePath());
      for (File file : listOfFiles != null ? listOfFiles : new File[0]) {
        if (file.isFile()) {
          String query;
          switch (fs) {
            case FS:
              query = readQueryFromFS(file.getAbsolutePath());
              break;
            case HDFS:
              query = readQueryFromHDFS(file.getAbsolutePath());
              break;
            default:
              throw new IOException(fs.toString());
          }
          list.add(new Tuple2<>(file.getName(), query));
        }
      }
    } else if (folder.isFile()) {
      String query = readQueryFromFS(folder.getAbsolutePath());
      list.add(new Tuple2<>(folder.getName(), query));
    }
    return list;
  }

  public static String readQuery(String path, FileSystemType fs)
      throws IOException {
    String query;
    switch (fs) {
      case FS:
        query = readQueryFromFS(path);
        break;
      case HDFS:
        query = readQueryFromHDFS(path);
        break;
      default:
        throw new IOException(fs.toString());
    }
    return query;
  }

  private static String readQueryFromFS(String file) {
    StringBuilder queryBuilder = new StringBuilder();
    BufferedReader br;
    try {
      br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        queryBuilder.append(line).append("\n");
      }
    } catch (Exception e) {
      System.err.println("File not found " + file);
    }
    return queryBuilder.toString();
  }

  private static String readQueryFromHDFS(String hdfsPath) {
    StringBuilder content = new StringBuilder();
    FileSystem fs;
    BufferedReader br;
    try {
      fs = FileSystem.get(new URI(hdfsPath), new Configuration());
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath))));
      String line;
      while ((line = br.readLine()) != null) {
        content.append(line).append("\n");
      }
    } catch (Exception e) {
      System.err.println("File not found " + hdfsPath);
    }
    return content.toString().trim();
  }
}
