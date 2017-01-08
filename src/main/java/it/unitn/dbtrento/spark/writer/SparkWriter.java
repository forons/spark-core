package it.unitn.dbtrento.spark.writer;

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
    switch (outputFormat) {
      case CSV:
        data.write().mode(SaveMode.Overwrite).format("csv").save(outputPath);
        return true;
      case PARQUET:
        data.write().mode(SaveMode.Overwrite).save(outputPath);
        return true;
      case SINGLE:
        throw new NotImplementedException("Method not yet implemented...");
      default:
        return false;
    }
  }
}
