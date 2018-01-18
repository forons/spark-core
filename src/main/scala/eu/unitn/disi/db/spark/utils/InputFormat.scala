package eu.unitn.disi.db.spark.utils

object InputFormat extends Enumeration {
  val CSV, PARQUET, TSV, SSV, JSON = Value

  implicit def getFieldDelimiter(format: Value): Char = format match {
    case CSV =>
      FieldDelimiter.COMMA.getFieldDelimiter
    case PARQUET =>
      FieldDelimiter.COMMA.getFieldDelimiter
    case TSV =>
      FieldDelimiter.TAB.getFieldDelimiter
    case SSV =>
      FieldDelimiter.SEMICOLON.getFieldDelimiter
    case _ =>
      System.err.println("Error while getting the field delimiter.")
      ','
  }
}
