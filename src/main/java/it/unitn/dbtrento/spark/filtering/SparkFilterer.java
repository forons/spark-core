package it.unitn.dbtrento.spark.filtering;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class SparkFilterer {
  public static Dataset<Row> applyFiltering(Dataset<Row> data,
      List<Tuple2<String, String>> whiteList, List<Tuple2<String, String>> blackList,
      List<String> colsToKeep) {
    Dataset<Row> support = data;
    support = SparkSelector.select(support, whiteList, blackList);
    support = SparkProjector.project(support, colsToKeep);
    return support;
  }
}
