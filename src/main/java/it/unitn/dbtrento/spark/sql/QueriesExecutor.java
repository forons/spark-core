package it.unitn.dbtrento.spark.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class QueriesExecutor {

  public static boolean registerTables(List<Tuple2<Dataset<Row>, String>> tablesName) {
    try {
      for (Tuple2<Dataset<Row>, String> tup : tablesName) {
        tup._1.createOrReplaceTempView(tup._2);
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public static Dataset<Row> executeQuery(SparkSession spark, String queryFilePath)
      throws IOException {
    Tuple2<String, String> tup =
        new Tuple2<>(new File(queryFilePath).getName(), readQueryFromFS(queryFilePath));
    Dataset<Row> result = spark.sql(tup._2);
    return result;
  }

  public static List<Dataset<Row>> executeQueries(SparkSession spark, String queriesFilePath)
      throws IOException {
    List<Tuple2<String, String>> queries = readQueriesFromFS(queriesFilePath);
    List<Dataset<Row>> resultList = new ArrayList<>();
    Dataset<Row> result;
    for (Tuple2<String, String> tup : queries) {
      result = spark.sql(tup._2);
      resultList.add(result);
    }
    return resultList;
  }

  public static List<Dataset<Row>> executeQueries(SparkSession spark,
      List<Tuple2<String, String>> queries) throws IOException {
    List<Dataset<Row>> resultList = new ArrayList<>();
    Dataset<Row> result;
    for (Tuple2<String, String> tup : queries) {
      result = spark.sql(tup._2);
      resultList.add(result);
    }
    return resultList;
  }

  private static List<Tuple2<String, String>> readQueriesFromFS(String queriesFilePath)
      throws IOException {
    File folder = new File(queriesFilePath);
    List<Tuple2<String, String>> list = new ArrayList<>();
    if (folder.isDirectory()) {
      File[] listOfFiles = folder.listFiles();
      System.out.println(folder.getAbsolutePath());
      for (File file : listOfFiles) {
        if (file.isFile()) {
          String query = readQueryFromFS(file.getAbsolutePath());
          list.add(new Tuple2<>(file.getName(), query));
        }
      }
    } else if (folder.isFile()) {
      String query = readQueryFromFS(folder.getAbsolutePath());
      list.add(new Tuple2<>(folder.getName(), query));
    }
    return list;
  }

  private static String readQueryFromFS(String file) throws FileNotFoundException, IOException {
    StringBuilder queryBuilder = new StringBuilder();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    while ((line = br.readLine()) != null) {
      queryBuilder.append(line + "\n");
    }
    br.close();
    return queryBuilder.toString();
  }


}
