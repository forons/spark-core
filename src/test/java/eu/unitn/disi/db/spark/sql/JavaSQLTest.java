package eu.unitn.disi.db.spark.sql;

import eu.unitn.disi.db.spark.io.Format;
import eu.unitn.disi.db.spark.io.FSType;
import eu.unitn.disi.db.spark.io.SparkReader;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSQLTest {

  @Test
  public void javaQueryExecutorTest() {
    SparkSession spark = SparkSession.builder().appName("test").master("local")
        .config("spark.driver.host", "localhost").getOrCreate();
    Dataset<Row> dataset = SparkReader
        .read(spark, true, true, Format.CSV, "src/test/resources/sample.csv");
    dataset.registerTempTable("tableTest");
    String query = "SELECT * FROM tableTest";
    QueryExecutor.executeQuery(spark, query).show();
    QueryExecutor.executeQuery(spark, "src/test/resources/query.txt", FSType.FS).show();
  }
}
