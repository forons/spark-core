package eu.unitn.disi.db.spark.utils

import org.slf4j.{Logger, LoggerFactory}

object OutputFormat extends Enumeration {

  val log : Logger = LoggerFactory.getLogger(this.getClass.getName)

  val CSV, PARQUET, SINGLE_CSV, JSON = Value

  implicit def getFieldDelimiter(format: Value): Char = format match {
    case CSV =>
      FieldDelimiter.COMMA.getFieldDelimiter
    case PARQUET =>
      FieldDelimiter.COMMA.getFieldDelimiter
    case SINGLE_CSV =>
      FieldDelimiter.TAB.getFieldDelimiter
    case _ =>
      log.error("Error while getting the field delimiter.")
      ','
  }
}
