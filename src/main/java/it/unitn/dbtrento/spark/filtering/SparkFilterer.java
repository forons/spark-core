package it.unitn.dbtrento.spark.filtering;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class SparkFilterer {
  
  /*
   * This function filters a dataset.
   * It takes as input a dataset, two lists (a whitelist and a blacklist), and a list of columns to keep.
   * The elements specified in the whitelist are the elements that will be kept,
   * while the elements defined in the blacklist will be discarded.
   */
  public static Dataset<Row> applyFiltering(Dataset<Row> data,
      List<Tuple2<String, String>> whiteList, List<Tuple2<String, String>> blackList,
      List<String> colsToKeep) {
    Dataset<Row> support = data;
    support = SparkSelector.select(support, whiteList, blackList);
    support = SparkProjector.projectByName(support, colsToKeep);
    return support;
  }
}
