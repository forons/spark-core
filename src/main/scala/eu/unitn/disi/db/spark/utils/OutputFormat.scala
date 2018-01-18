package eu.unitn.disi.db.spark.utils

object OutputFormat extends Enumeration {
  val CSV, PARQUET, SINGLE_CSV, JSON = Value
}
