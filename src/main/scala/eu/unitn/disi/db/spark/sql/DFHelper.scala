package eu.unitn.disi.db.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object DFHelper {

  def addIndexColumn(spark: SparkSession,
                     dataset: Dataset[Row],
                     idColumnName: String): Dataset[Row] = {
    val rows: RDD[Row] = dataset.rdd
      .zipWithIndex()
      .map {
        case (row, idx) => Row.fromSeq(Seq(idx) ++ row.toSeq)
      }
    val schema = StructType(
      Array(StructField(idColumnName, LongType, nullable = true)) ++ dataset.schema.fields
    )
    spark.createDataFrame(rows, schema)
  }
}
