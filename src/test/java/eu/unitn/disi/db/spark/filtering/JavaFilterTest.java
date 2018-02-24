package eu.unitn.disi.db.spark.filtering;

import eu.unitn.disi.db.spark.io.SparkReader;
import eu.unitn.disi.db.spark.io.SparkWriter;
import eu.unitn.disi.db.spark.io.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.junit.Test;
import scala.Tuple2;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaFilterTest {

  @Test
  public void javaFilterListTest() {
    SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    Dataset<Row> dataset = SparkReader
        .read(spark, true, true, Format.CSV, "src/test/resources/sample.csv");
    List<Tuple2> whiteList = new ArrayList<>();
    whiteList.add(new Tuple2<>("city", "san francisco"));
    whiteList.add(new Tuple2<>(0, 1));
    List<Tuple2> blackList = new ArrayList<>();
    blackList.add(new Tuple2<>("name", "la taqueria"));
    blackList.add(new Tuple2<>(0, 11));
    List<String> colsToKeep = Arrays.asList("0", "city", "name");

    Dataset results = Filter.applyFilter(dataset, whiteList, blackList, colsToKeep);
    results.show();

    Map<String, String> options = new HashMap<>();
    options.put("header", "true");

    SparkWriter
        .write(spark, results, options, "out/out.csv", Format.CSV);
  }

  @Test
  public void javaFilterMapTest() {
    SparkSession spark = SparkSession.builder().appName("test").master("local")
        .config("spark.driver.host", "localhost").getOrCreate();
    Dataset<Row> dataset = SparkReader
        .read(spark, true, true, Format.CSV, "src/test/resources/sample.csv");
    Map<Object, List<Object>> whiteList = new HashMap<>();
    whiteList.put("city", Collections.singletonList("san francisco"));
    whiteList.put(0, Collections.singletonList(1));
    Map<Object, List<Object>> blackList = new HashMap<>();
    blackList.put("name", Collections.singletonList("la taqueria"));
    blackList.put(0, Collections.singletonList(11));

    List<String> colsToKeep = Arrays.asList("0", "city", "name");

    Dataset results = Filter.applyFilter(dataset, whiteList, blackList, colsToKeep);
    results.show();

    Map<String, String> options = new HashMap<>();
    options.put("header", "true");

    SparkWriter
        .write(spark, results, options, "out/out.csv", Format.CSV);
  }
}
