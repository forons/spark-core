package it.unitn.dbtrento.spark.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.unitn.dbtrento.spark.utils.InputFormat;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class SparkReader {
  public static Dataset<Row> read(SparkSession spark, String inputPath, boolean hasHeader,
      InputFormat inputFormat) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.err.println("The input path is empty, thus it can not be read...");
      return null;
    }

    Map<String, String> options = new HashMap<>();
    options.put("inferSchema", "false");
    options.put("header", String.valueOf(hasHeader));
    Dataset<Row> data = null;

    switch (inputFormat) {
      case CSV:
        data = spark.read().options(options).csv(inputPath);
        break;
      case PARQUET:
        data = spark.read().options(options).load(inputPath);
        break;
      case TSV:
        options.put("sep", "\t");
        data = spark.read().options(options).csv(inputPath);
        break;
      case SSV:
        options.put("sep", ";");
        data = spark.read().options(options).csv(inputPath);
        break;
      default:
        break;
    }
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, Seq<String> inputPath, boolean hasHeader,
      InputFormat inputFormat) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.err.println("The input path is empty, thus it can not be read...");
      return null;
    }

    Map<String, String> options = new HashMap<>();
    options.put("inferSchema", "false");
    options.put("header", String.valueOf(hasHeader));
    Dataset<Row> data = null;

    switch (inputFormat) {
      case CSV:
        data = spark.read().options(options).csv(inputPath);
        break;
      case PARQUET:
        data = spark.read().options(options).load(inputPath);
        break;
      case TSV:
        options.put("sep", "\t");
        data = spark.read().options(options).csv(inputPath);
        break;
      case SSV:
        options.put("sep", ";");
        data = spark.read().options(options).csv(inputPath);
        break;
      default:
        break;
    }
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, List<String> inputPath, boolean hasHeader,
      InputFormat inputFormat) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.err.println("The input path is empty, thus it can not be read...");
      return null;
    }

    Map<String, String> options = new HashMap<>();
    options.put("inferSchema", "false");
    options.put("header", String.valueOf(hasHeader));
    Dataset<Row> data = null;
    Seq<String> elements = JavaConversions.asScalaBuffer(inputPath).seq();
    try {
      switch (inputFormat) {
        case CSV:
          data = spark.read().options(options).csv(elements);
          break;
        case PARQUET:
          data = spark.read().options(options).load(elements);
          break;
        case TSV:
          options.put("sep", "\t");
          data = spark.read().options(options).csv(elements);
          break;
        case SSV:
          options.put("sep", ";");
          data = spark.read().options(options).csv(elements);
          break;
        default:
          break;
      }
    } catch (UnsupportedOperationException e) {
      System.err.println(e.getMessage() + " while reading the data from " + inputPath.toString());
    }
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, boolean hasHeader, InputFormat inputFormat,
      String... inputPath) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    }

    Map<String, String> options = new HashMap<>();
    options.put("inferSchema", "false");
    options.put("header", String.valueOf(hasHeader));
    Dataset<Row> data = null;

    switch (inputFormat) {
      case CSV:
        data = spark.read().options(options).csv(inputPath);
        break;
      case PARQUET:
        data = spark.read().options(options).load(inputPath);
        break;
      case TSV:
        options.put("sep", "\t");
        data = spark.read().options(options).csv(inputPath);
        break;
      case SSV:
        options.put("sep", ";");
        data = spark.read().options(options).csv(inputPath);
        break;
      default:
        break;
    }
    return data;
  }
}
