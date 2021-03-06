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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaFilterTest {

  private SparkSession spark;
  private Dataset<Row> dataset, results;
  private Map<String, String> options;

  @Before
  public void init() {
    spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    options = new HashMap<>();
    options.put("header", "true");
    dataset = SparkReader
        .read(spark, true, true, Format.CSV, "src/test/resources/sample.csv");
  }

  @After
  public void stop() {
    if (results != null) {
      results.show();
    }
    SparkWriter.write(spark, results, options, "out/out.csv", Format.CSV);
  }

  @Test
  public void javaFilterListTest() {
    List<Tuple2> whiteList = new ArrayList<>();
    whiteList.add(new Tuple2<>("city", "san francisco"));
    whiteList.add(new Tuple2<>(0, 1));

    List<Tuple2> blackList = new ArrayList<>();
    blackList.add(new Tuple2<>("name", "la taqueria"));
    blackList.add(new Tuple2<>(0, 11));

    List<String> colsToKeep = Arrays.asList("0", "city", "name");

    results = Filter.applyFilter(dataset, whiteList, blackList, colsToKeep);
  }

  @Test
  public void javaFilterMapTest() {
    Map<Object, List<Object>> whiteList = new HashMap<>();
    whiteList.put("city", Collections.singletonList("san francisco"));
    whiteList.put(0, Collections.singletonList(1));

    Map<Object, List<Object>> blackList = new HashMap<>();
    blackList.put("name", Collections.singletonList("la taqueria"));
    blackList.put(0, Collections.singletonList(11));

    List<String> colsToKeep = Arrays.asList("0", "city", "name");

    results = Filter.applyFilter(dataset, whiteList, blackList, colsToKeep);
  }
}
