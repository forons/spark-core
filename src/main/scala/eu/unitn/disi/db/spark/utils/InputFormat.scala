package eu.unitn.disi.db.spark.utils

import org.slf4j.{Logger, LoggerFactory}

object InputFormat extends Enumeration {

  val log : Logger = LoggerFactory.getLogger(this.getClass.getName)

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
      log.error("Error while getting the field delimiter.")
      ','
  }
}
