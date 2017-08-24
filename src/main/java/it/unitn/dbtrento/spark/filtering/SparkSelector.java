package it.unitn.dbtrento.spark.filtering;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class SparkSelector {
  
  /*
   * This function receives a dataset, a whitelist, and a blacklist.
   * The two lists are defined as a list of tuples where the first element is the column 
   * and the second element is the value that should be keep (white list) or discard (black list).
   */
  protected static Dataset<Row> select(Dataset<Row> data, List<Tuple2<String, String>> whiteList,
      List<Tuple2<String, String>> blackList) {
    if(data == null) {
      return null;
    }
    Dataset<Row> support = data;
    if (blackList != null) {
      support = applyBlackList(support, blackList);
    }
    if (whiteList != null) {
      support = applyWhiteList(support, whiteList);
    }
    return support;
  }

  /*
   * This function receives a dataset and a whitelist.
   * The list is defined as a list of tuples where the first element is the column 
   * and the second element is the value that should be keep.
   */
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

  /*
   * This function receives a dataset and a blacklist.
   * The list is defined as a list of tuples where the first element is the column 
   * and the second element is the value that should be discard.
   */
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
