package eu.unitn.disi.db.spark.utils

import eu.unitn.disi.db.spark.io.Format
import scala.collection.JavaConversions

object Utils {

  def addSeparatorToOptions(options: Map[String, String],
                            format: Format): Map[String, String] =
    if (options == null) {
      Map("sep" -> format.getFieldDelimiter.toString)
    } else {
      options + ("sep" -> format.getFieldDelimiter.toString)
    }

  def javaToScalaTupleList(list: java.util.List[_]): List[(_, _)] =
    JavaConversions.asScalaBuffer(list).toList.asInstanceOf[List[(_, _)]]

  //  def javaToScalaMap(map: java.util.Map[_, _]): Map[_, _] =
  //    JavaConversions.mapAsScalaMap(map).toMap

  def javaToScalaList(list: java.util.List[_]): List[_] =
    JavaConversions.asScalaBuffer(list).toList

  def javaToScalaMap[T, U](map: java.util.Map[T, U]): Map[T, U] =
    JavaConversions.mapAsScalaMap[T, U](map).toMap

}
