package eu.unitn.disi.db.spark.utils

import eu.unitn.disi.db.spark.io.Format
import scala.collection.JavaConversions

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
    else JavaConversions.asScalaBuffer(list).toList.asInstanceOf[List[(_, _)]]

  //  def javaToScalaMap(map: java.util.Map[_, _]): Map[_, _] =
  //    JavaConversions.mapAsScalaMap(map).toMap

  def javaToScalaList(list: java.util.List[_]): List[_] =
    if (list == null) null
    else JavaConversions.asScalaBuffer(list).toList

  def javaToScalaMap[T, U](map: java.util.Map[T, U]): Map[T, U] =
    if (map == null) null
    else JavaConversions.mapAsScalaMap[T, U](map).toMap
}
