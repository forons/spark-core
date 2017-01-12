package it.unitn.dbtrento.spark.writer;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import it.unitn.dbtrento.spark.utils.OutputFormat;

public class SparkWriter {

  public static boolean writeResults(SparkSession spark, Dataset<Row> data, String outputPath,
      boolean hasHeader, OutputFormat outputFormat) {
    if (data == null) {
      System.out.println("Not able to write the data...");
      return false;
    } else if (outputPath == null) {
      System.out.println("Not able to write the data...");
      return false;
    } else if (outputPath.isEmpty()) {
      System.out.println("Not able to write the data...");
      return false;
    }
    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    switch (outputFormat) {
      case CSV:
        data.write().options(options).mode(SaveMode.Overwrite).csv(outputPath);
        return true;
      case PARQUET:
        data.write().options(options).mode(SaveMode.Overwrite).save(outputPath);
        return true;
      case SINGLE:
        throw new NotImplementedException("Method not yet implemented...");
      default:
        return false;
    }
  }
}
