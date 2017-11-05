package it.unitn.dbtrento.spark.writer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import it.unitn.dbtrento.spark.utils.OutputFormat;

public class SparkWriter {

  public static <T> boolean write(SparkSession spark, Dataset<T> data, boolean hasHeader,
      String outputPath, OutputFormat outputFormat) {
    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    return write(spark, data, options, outputPath, outputFormat);
  }

  public static <T> boolean write(SparkSession spark, Dataset<T> data, boolean hasHeader,
      String outputPath, OutputFormat outputFormat, String outputFile) {
    Map<String, String> options = new HashMap<>();
    options.put("header", String.valueOf(hasHeader));
    return write(spark, data, options, outputPath, outputFormat, outputFile);
  }

  public static <T> boolean write(SparkSession spark, Dataset<T> data, Map<String, String> options,
      String outputPath, OutputFormat outputFormat) {
    return write(spark, data, options, outputPath, outputFormat, "output");
  }

  public static <T> boolean write(SparkSession spark, Dataset<T> data, Map<String, String> options,
      String outputPath, OutputFormat outputFormat, String outputFile) {
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
    try {
      switch (outputFormat) {
        case CSV:
          data.write().options(options).mode(SaveMode.Overwrite).csv(outputPath);
          return true;
        case PARQUET:
          data.write().options(options).mode(SaveMode.Overwrite).save(outputPath);
          return true;
        case SINGLE_CSV:
          data.coalesce(1).write().options(options).mode(SaveMode.Overwrite)
              .csv(outputPath + "/partial/");
          try {
            FileUtil.copyMerge(
                FileSystem.get(new URI(outputPath + "/partial/"), new Configuration()),
                new Path(outputPath + "/partial/"),
                FileSystem.get(new URI(outputPath + "/" + outputFile + ".csv"),
                    new Configuration()),
                new Path(outputPath + "/" + outputFile + ".csv"), true,
                spark.sparkContext().hadoopConfiguration(), null);
          } catch (IllegalArgumentException | IOException | URISyntaxException e) {
            System.err.println("Error " + e.getMessage() + "while writing the data");
            e.printStackTrace();
          }
          return true;
        default:
          return false;
      }
    } catch (UnsupportedOperationException e) {
      System.err
          .printf("%s while writing the data to %s in %s format.\n", e.getMessage(), outputPath,
              outputFormat.toString());
    }
    return false;
  }
}
