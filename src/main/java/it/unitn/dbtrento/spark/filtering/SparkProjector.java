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
  protected static Dataset<Row> project(Dataset<Row> data, List<String> colsToKeep) {
    String[] cols = data.columns();
    List<Column> colsToKeepNames = new ArrayList<>();
    if (colsToKeep == null) {
      return data;
    }
    for (String col : colsToKeep) {
      String c = null;
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
