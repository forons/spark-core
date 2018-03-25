package eu.unitn.disi.db.spark.utils

import eu.unitn.disi.db.spark.io.Format
import scala.collection.JavaConverters._

object Utils {

  def addSeparatorToOptions(options: Map[String, String],
                            format: Format): Map[String, String] =
    if (format == null) {
      Map.empty
    } else if (options == null) {
      Map("sep" -> format.getFieldDelimiter.toString)
    } else {
      options + ("sep" -> format.getFieldDelimiter.toString)
    }

  def javaToScalaTupleList(list: java.util.List[_]): List[(_, _)] =
    if (list == null) null
    else list.asScala.toList.asInstanceOf[List[(_, _)]]

  def javaToScalaList[U](list: java.util.List[U]): List[U] =
    if (list == null) null
    else list.asScala.toList

  def javaToScalaMap[T, U](map: java.util.Map[T, U]): Map[T, U] =
    if (map == null) null
    else map.asScala.toMap
}
