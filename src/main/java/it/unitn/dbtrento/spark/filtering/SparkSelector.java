package it.unitn.dbtrento.spark.filtering;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class SparkSelector {
  protected static Dataset<Row> select(Dataset<Row> data, List<Tuple2<String, String>> whiteList,
      List<Tuple2<String, String>> blackList) {
    Dataset<Row> support = data;
    if (blackList != null) {
      support = applyBlackList(support, blackList);
    }
    if (whiteList != null) {
      support = applyWhiteList(support, whiteList);
    }
    return support;
  }

  private static Dataset<Row> applyWhiteList(Dataset<Row> data,
      List<Tuple2<String, String>> whiteList) {
    Dataset<Row> support = data;
    support = support.filter(row -> {
      for (Tuple2<String, String> tup : whiteList) {
        String elem = "";
        try {
          int colIndex = Integer.parseInt(tup._1);
          elem = row.getString(colIndex);
        } catch (NumberFormatException e) {
          elem = row.getAs(tup._1);
        }
        if (elem.isEmpty()) {
          return false;
        }
        if (elem.equals(tup._2)) {
          return true;
        }
      }
      return false;
    });
    return support;
  }

  private static Dataset<Row> applyBlackList(Dataset<Row> data,
      List<Tuple2<String, String>> blackList) {
    Dataset<Row> support = data;
    for (Tuple2<String, String> tup : blackList) {
      String column = "";
      try {
        int colIndex = Integer.parseInt(tup._1);
        column = support.columns()[colIndex];
      } catch (NumberFormatException e) {
        column = tup._1;
      }
      support = support.filter(col(column).notEqual(tup._2));

    }
    return support;
  }
}
