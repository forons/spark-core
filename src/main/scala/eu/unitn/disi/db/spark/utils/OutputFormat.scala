package eu.unitn.disi.db.spark.utils

object OutputFormat extends Enumeration {
  val CSV, PARQUET, SINGLE_CSV, JSON = Value

  implicit def getFieldDelimiter(format: Value): Char = format match {
    case CSV =>
      FieldDelimiter.COMMA.getFieldDelimiter
    case PARQUET =>
      FieldDelimiter.COMMA.getFieldDelimiter
    case SINGLE_CSV =>
      FieldDelimiter.TAB.getFieldDelimiter
    case _ =>
      System.err.println("Error while getting the field delimiter.")
      ','
  }
}
