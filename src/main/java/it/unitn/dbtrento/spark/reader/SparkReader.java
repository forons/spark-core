package it.unitn.dbtrento.spark.reader;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.unitn.dbtrento.spark.utils.InputFormat;

public class SparkReader {
  public static Dataset<Row> read(SparkSession spark, String inputPath, boolean hasHeader,
      InputFormat inputFormat) {
    if (inputPath == null) {
      System.out.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.out.println("The input path is empty, thus it can not be read...");
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

  public static Dataset<Row> read(SparkSession spark, String inputPath, String regex,
      boolean hasHeader, InputFormat inputFormat) {
    if (inputPath == null) {
      System.out.println("Not able to read the data...");
      return null;
    } else if (inputPath.isEmpty()) {
      System.out.println("The input path is empty, thus it can not be read...");
      return null;
    }

    Map<String, String> options = new HashMap<>();
    options.put("inferSchema", "false");
    options.put("header", String.valueOf(hasHeader));
    Dataset<Row> data = null;

    switch (inputFormat) {
      case CSV:
        data = spark.read().options(options).csv(inputPath + "/" + regex);
        break;
      case PARQUET:
        data = spark.read().options(options).load(inputPath + "/" + regex);
        break;
      case TSV:
        options.put("sep", "\t");
        data = spark.read().options(options).csv(inputPath + "/" + regex);
        break;
      case SSV:
        options.put("sep", ";");
        data = spark.read().options(options).csv(inputPath + "/" + regex);
        break;
      default:
        break;
    }
    return data;
  }
}
