package eu.unitn.disi.db.spark.filtering;

import eu.unitn.disi.db.spark.io.SparkReader;
import eu.unitn.disi.db.spark.io.SparkWriter;
import eu.unitn.disi.db.spark.utils.InputFormat;
import eu.unitn.disi.db.spark.utils.OutputFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

public class JavaFilterTest {

  @Test
  public void testJavaFilter() {
    SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    Dataset<Row> dataset = SparkReader
        .read(spark, true, true, InputFormat.CSV(), "/Users/forons/Desktop/test_sample.csv");
    List<Tuple2> whiteList = new ArrayList<>();
    whiteList.add(new Tuple2<>("city", "san francisco"));
    whiteList.add(new Tuple2<>(0, 1));
    List<Tuple2> blackList = new ArrayList<>();
    blackList.add(new Tuple2<>("name", "la taqueria"));
    blackList.add(new Tuple2<>(0, 11));
    List<String> colsToKeep = Arrays.asList("0", "city", "name");

    Dataset results = JavaFilter.applyFilter(dataset, whiteList, blackList, colsToKeep);
    results.show();

    Map<String, String> options = new HashMap<>();
    options.put("header", "true");

    SparkWriter
        .write(spark, results, options, "/Users/forons/Desktop/bbbb.csv", OutputFormat.CSV());
  }
}
