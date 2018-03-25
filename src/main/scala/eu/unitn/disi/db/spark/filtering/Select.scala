package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.utils.Utils

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

object Select {

  def applySelect[U](dataset: Dataset[U],
                     whiteList: java.util.List[_],
                     blackList: java.util.List[_]): Dataset[U] = {
    Select.applySelect(
      dataset,
      Utils.javaToScalaTupleList(whiteList),
      Utils.javaToScalaTupleList(blackList))
  }

  def applySelect[U](dataset: Dataset[U],
                     whiteList: java.util.Map[_, _],
                     blackList: java.util.Map[_, _]): Dataset[U] = {
    Select.applySelect(dataset, Utils.javaToScalaMap(whiteList), Utils.javaToScalaMap(blackList))
  }

  def applySelect[U](dataset: Dataset[U],
                     whiteList: java.util.List[_],
                     blackList: java.util.Map[_, _]): Dataset[U] = {
    Select.applySelect(
      dataset,
      Utils.javaToScalaTupleList(whiteList),
      Utils.javaToScalaMap(blackList))
  }

  def applySelect[U](dataset: Dataset[U],
                     whiteList: java.util.Map[_, _],
                     blackList: java.util.List[_]): Dataset[U] = {
    Select.applySelect(
      dataset,
      Utils.javaToScalaMap(whiteList),
      Utils.javaToScalaTupleList(blackList))
  }

  def applyBlackList[U](dataset: Dataset[U],
                        blackList: java.util.List[_]): Dataset[U] = {
    Select.applyBlackList(dataset, Utils.javaToScalaTupleList(blackList))
  }

  def applyBlackList[U](dataset: Dataset[U],
                        blackList: java.util.Map[_, _]): Dataset[U] = {
    Select.applyBlackList(dataset, Utils.javaToScalaMap(blackList))
  }

  def applyWhiteList[U](dataset: Dataset[U],
                        whiteList: java.util.List[_]): Dataset[U] = {
    Select.applyWhiteList(dataset, Utils.javaToScalaTupleList(whiteList))
  }

  def applyWhiteList[U](dataset: Dataset[U],
                        whiteList: java.util.Map[_, _]): Dataset[U] = {
    Select.applyWhiteList(dataset, Utils.javaToScalaMap(whiteList))
  }

  def applySelect[U](dataset: Dataset[U],
                     whiteList: List[(_, _)],
                     blackList: List[(_, _)]): Dataset[U] = {
    if (dataset == null) return null
    var support: Dataset[U] = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def applySelect[U](dataset: Dataset[U],
                     whiteList: Map[_, _],
                     blackList: Map[_, _]): Dataset[U] = {
    if (dataset == null) return null
    var support: Dataset[U] = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def applySelect[U](dataset: Dataset[U],
                     whiteList: List[(_, _)],
                     blackList: Map[_, _]): Dataset[U] = {
    val support: Dataset[U] = applyBlackList(dataset, blackList)
    applyWhiteList(support, whiteList)
  }

  def applySelect[U](dataset: Dataset[U],
                     whiteList: Map[_, _],
                     blackList: List[(_, _)]): Dataset[U] = {
    val support: Dataset[U] = applyBlackList(dataset, blackList)
    applyWhiteList(support, whiteList)
  }

  def matchTuple[U](dataset: Dataset[U], item: Any): String = {
    item match {
      case _: Int => dataset.columns(item.asInstanceOf[Int])
      case _: String => item.asInstanceOf[String]
      case _ => throw new IllegalArgumentException("This should not happened")
    }
  }

  def applyBlackList[U](dataset: Dataset[U],
                        blackList: List[(_, _)]): Dataset[U] = {
    if (blackList == null || blackList.isEmpty || dataset == null) return dataset
    var condition =
      col(matchTuple(dataset, blackList.head._1)).notEqual(blackList.head._2)
    for (tup <- blackList) {
      condition =
        condition.and(col(matchTuple(dataset, tup._1)).notEqual(tup._2))
    }
    dataset.filter(condition)
  }

  def applyBlackList[U](dataset: Dataset[U], blackList: Map[_, _]): Dataset[U] = {
    applyBlackList(dataset, fromMapToList(blackList))
  }

  def applyWhiteList[U](dataset: Dataset[U],
                        whiteList: List[(_, _)]): Dataset[U] = {
    if (whiteList == null || whiteList.isEmpty || dataset == null) return dataset
    var condition =
      col(matchTuple(dataset, whiteList.head._1)).equalTo(whiteList.head._2)
    for (tup <- whiteList) {
      condition = condition.or(col(matchTuple(dataset, tup._1)).equalTo(tup._2))
    }
    dataset.filter(condition)
  }

  def applyWhiteList[U](dataset: Dataset[U], whiteList: Map[_, _]): Dataset[U] = {
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
