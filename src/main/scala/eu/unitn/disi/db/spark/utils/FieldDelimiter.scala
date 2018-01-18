package eu.unitn.disi.db.spark.utils

object FieldDelimiter extends Enumeration {

  protected case class Val(fieldDelimiter: Char) extends super.Val {
    def getFieldDelimiter: Char = fieldDelimiter
  }

  implicit def valueToFieldDelimiterVal(fieldDelimiter: Value) =
    fieldDelimiter.asInstanceOf[Val]

  val COMMA = Val(',')
  val TAB = Val('\t')
  val SPACE = Val(' ')
  val COLON = Val(':')
  val SEMICOLON = Val(';')
}
