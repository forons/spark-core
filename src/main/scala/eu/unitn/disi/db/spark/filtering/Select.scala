package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.utils.Utils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

object Select {

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.List[_],
                  blackList: java.util.List[_]): Dataset[_] = {
    Select.applySelect(dataset, Utils.javaToScalaTupleList(whiteList), Utils.javaToScalaTupleList(blackList))
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.Map[_, _],
                  blackList: java.util.Map[_, _]): Dataset[_] = {
    Select.applySelect(dataset, Utils.javaToScalaMap(whiteList), Utils.javaToScalaMap(blackList))
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.List[_],
                  blackList: java.util.Map[_, _]): Dataset[_] = {
    Select.applySelect(dataset, Utils.javaToScalaTupleList(whiteList), Utils.javaToScalaMap(blackList))
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.Map[_, _],
                  blackList: java.util.List[_]): Dataset[_] = {
    Select.applySelect(dataset, Utils.javaToScalaMap(whiteList), Utils.javaToScalaTupleList(blackList))
  }

  def applyBlackList(dataset: Dataset[_],
                     blackList: java.util.List[_]): Dataset[_] = {
    Select.applyBlackList(dataset, Utils.javaToScalaTupleList(blackList))
  }

  def applyBlackList(dataset: Dataset[_],
                     blackList: java.util.Map[_, _]): Dataset[_] = {
    Select.applyBlackList(dataset, Utils.javaToScalaMap(blackList))
  }

  def applyWhiteList(dataset: Dataset[_],
                     whiteList: java.util.List[_]): Dataset[_] = {
    Select.applyWhiteList(dataset, Utils.javaToScalaTupleList(whiteList))
  }

  def applyWhiteList(dataset: Dataset[_],
                     whiteList: java.util.Map[_, _]): Dataset[_] = {
    Select.applyWhiteList(dataset, Utils.javaToScalaMap(whiteList))
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: List[(_, _)],
                  blackList: List[(_, _)]): Dataset[_] = {
    if (dataset == null) return null
    var support: Dataset[_] = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: Map[_, _],
                  blackList: Map[_, _]): Dataset[_] = {
    if (dataset == null) return null
    var support: Dataset[_] = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: List[(_, _)],
                  blackList: Map[_, _]): Dataset[_] = {
    val support: Dataset[_] = applyBlackList(dataset, blackList)
    applyWhiteList(support, whiteList)
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: Map[_, _],
                  blackList: List[(_, _)]): Dataset[_] = {
    val support: Dataset[_] = applyBlackList(dataset, blackList)
    applyWhiteList(support, whiteList)
  }

  def matchTuple(dataset: Dataset[_], item: Any): String = {
    item match {
      case _: Int => dataset.columns(item.asInstanceOf[Int])
      case _: String => item.asInstanceOf[String]
      case _ => throw new IllegalArgumentException("This should not happened")
    }
  }

  def applyBlackList(dataset: Dataset[_],
                     blackList: List[(_, _)]): Dataset[_] = {
    if (blackList == null || blackList.isEmpty || dataset == null) return dataset
    var condition =
      col(matchTuple(dataset, blackList.head._1)).notEqual(blackList.head._2)
    for (tup <- blackList) {
      condition =
        condition.and(col(matchTuple(dataset, tup._1)).notEqual(tup._2))
    }
    dataset.filter(condition)
  }

  def applyBlackList(dataset: Dataset[_], blackList: Map[_, _]): Dataset[_] = {
    applyBlackList(dataset, fromMapToList(blackList))
  }

  def applyWhiteList(dataset: Dataset[_],
                     whiteList: List[(_, _)]): Dataset[_] = {
    if (whiteList == null || whiteList.isEmpty || dataset == null) return dataset
    var condition =
      col(matchTuple(dataset, whiteList.head._1)).equalTo(whiteList.head._2)
    for (tup <- whiteList) {
      condition = condition.or(col(matchTuple(dataset, tup._1)).equalTo(tup._2))
    }
    dataset.filter(condition)
  }

  def applyWhiteList(dataset: Dataset[_], whiteList: Map[_, _]): Dataset[_] = {
    applyWhiteList(dataset, fromMapToList(whiteList))
  }

  def fromMapToList(map: Map[_, _]): List[(_, _)] = {
    var list: List[(_, _)] = List()
    for (tup <- map) {
      tup._2 match {
        case elems: Array[_] =>
          for (elem <- elems) {
            list = (tup._1, elem) :: list
          }
        case elems: List[_] =>
          for (elem <- elems) {
            list = (tup._1, elem) :: list
          }
        case elems: java.util.List[_] =>
          for (elem <- Utils.javaToScalaList(elems)) {
            list = (tup._1, elem) :: list
          }
        case elem => list = (tup._1, elem) :: list
      }
    }
    list
  }
}
