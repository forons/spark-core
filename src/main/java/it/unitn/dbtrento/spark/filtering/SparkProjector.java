package it.unitn.dbtrento.spark.filtering;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.collection.JavaConversions;
import scala.collection.Seq;

public class SparkProjector {

  /*
   * This function receives a dataset and a set of columns that should be returned. The columns can
   * be specified as a set of column names or column indexes.
   */
  public static Dataset<Row> projectByName(Dataset<Row> data, List<String> colsToKeep) {
    if (data == null || colsToKeep == null || data.count() == 0L) {
      return data;
    }
    String[] cols = data.columns();
    List<Column> colsToKeepNames = new ArrayList<>();
    for (String col : colsToKeep) {
      String c;
      try {
        c = cols[Integer.parseInt(col)];
      } catch (NumberFormatException e) {
        c = col;
      }
      colsToKeepNames.add(col(c));
    }
    Seq<Column> colsSeq = JavaConversions.asScalaBuffer(colsToKeepNames).toList();
    return data.select(colsSeq);
  }
}
