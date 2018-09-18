package eu.unitn.disi.db.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

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

  def getColumnType(df: Dataset[Row], column: String): DataType =
    getColumnType(df, df.columns.indexOf(column))

  def getColumnType(df: Dataset[Row], colIndex: Int): DataType =
    df.schema.fields(colIndex).dataType

  def isNumeric(df: Dataset[Row], column: String): Boolean =
    isNumeric(getColumnType(df, column))

  val numericDataTypes: Seq[DataType] =
    Seq(BinaryType, IntegerType, LongType, DoubleType, FloatType, ShortType)

  def isNumeric(dataType: DataType): Boolean = {
    numericDataTypes.contains(dataType)
  }
}
