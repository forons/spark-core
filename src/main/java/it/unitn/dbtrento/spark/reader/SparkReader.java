package it.unitn.dbtrento.spark.reader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.unitn.dbtrento.spark.utils.InputFormat;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class SparkReader {
  public static Dataset<Row> read(SparkSession spark, boolean hasHeader, boolean inferSchema,
      InputFormat inputFormat, String inputPath) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.err.println("The input path is empty, thus it can not be read...");
      return null;
    }
    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    options.put("inferSchema", String.valueOf(inferSchema));
    Dataset<Row> data = read(spark, options, inputFormat, inputPath);
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, boolean hasHeader, boolean inferSchema,
      InputFormat inputFormat, Seq<String> inputPath) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.err.println("The input path is empty, thus it can not be read...");
      return null;
    }
    Configuration configuration = new Configuration();
    configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    configuration.set("fs.file.impl", LocalFileSystem.class.getName());

    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    options.put("inferSchema", String.valueOf(inferSchema));

    Dataset<Row> data = read(spark, options, inputFormat, inputPath);
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, boolean hasHeader, boolean inferSchema,
      InputFormat inputFormat, List<String> inputPath) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.err.println("The input path is empty, thus it can not be read...");
      return null;
    }
    Configuration configuration = new Configuration();
    configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    configuration.set("fs.file.impl", LocalFileSystem.class.getName());

    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    options.put("inferSchema", String.valueOf(inferSchema));
    Seq<String> elements = JavaConversions.asScalaBuffer(inputPath).seq();
    Dataset<Row> data = read(spark, options, inputFormat, elements);
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, boolean hasHeader, boolean inferSchema,
      InputFormat inputFormat, String... inputPath) {
    if (inputPath == null) {
      System.err.println("Not able to read the data...");
      return null;
    }
    Configuration configuration = new Configuration();
    configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    configuration.set("fs.file.impl", LocalFileSystem.class.getName());

    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    options.put("inferSchema", String.valueOf(inferSchema));
    Dataset<Row> data = read(spark, options, inputFormat, inputPath);
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, Map<String, String> options,
      InputFormat inputFormat, Seq<String> inputPath) {
    Dataset<Row> data = null;
    try {
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
    } catch (UnsupportedOperationException e) {
      System.err.println(e.getMessage() + " while reading the data from " + inputPath.toString());
    }
    return data;
  }

  public static Dataset<Row> read(SparkSession spark, Map<String, String> options,
      InputFormat inputFormat, String... inputPath) {
    Dataset<Row> data = null;
    try {
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
    } catch (UnsupportedOperationException e) {
      System.err
          .println(e.getMessage() + " while reading the data from " + Arrays.toString(inputPath));
    }
    return data;
  }
}
