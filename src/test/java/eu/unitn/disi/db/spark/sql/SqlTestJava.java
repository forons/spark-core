package eu.unitn.disi.db.spark.sql;

import eu.unitn.disi.db.spark.io.FSType;
import eu.unitn.disi.db.spark.io.SparkReader;

import eu.unitn.disi.db.spark.utils.InputFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class SqlTestJava {

  @Test
  public void testQueryExecutorJava() {
    SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    Dataset<Row> dataset = SparkReader
        .read(spark, true, true, InputFormat.CSV(), "src/test/resources/sample.csv");
    dataset.registerTempTable("tableTest");
    String query = "SELECT * FROM tableTest";
    QueryExecutor.executeQuery(spark, query).show();
    QueryExecutor.executeQuery(spark, "src/test/resources/query.txt", FSType.FS()).show();
  }
}
