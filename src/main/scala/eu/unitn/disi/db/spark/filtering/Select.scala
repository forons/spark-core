package eu.unitn.disi.db.spark.filtering

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import scala.collection.JavaConversions

object Select {

  def applySelect(dataset: Dataset[_],
                  whiteList: List[(_, _)],
                  blackList: List[(_, _)]): Dataset[_] = {
    if (dataset == null) return null
    var support = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: Map[_, _],
                  blackList: Map[_, _]): Dataset[_] = {
    if (dataset == null) return null
    var support = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: List[(_, _)],
                  blackList: Map[_, _]): Dataset[_] = {
    if (dataset == null) return null
    var support = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: Map[_, _],
                  blackList: List[(_, _)]): Dataset[_] = {
    if (dataset == null) return null
    var support = dataset
    if (blackList != null) support = applyBlackList(support, blackList)
    if (whiteList != null) support = applyWhiteList(support, whiteList)
    support
  }

  def matchTuple(dataset: Dataset[_], item: Any): String = {
    item match {
      case _: Int    => dataset.columns(item.asInstanceOf[Int])
      case _: String => item.asInstanceOf[String]
      case _         => throw new IllegalArgumentException("This should not happened")
    }
  }

  def applyBlackList(dataset: Dataset[_],
                     blackList: List[(_, _)]): Dataset[_] = {
    if (blackList == null || blackList.isEmpty) return dataset
    var condition =
      col(matchTuple(dataset, blackList.head._1)).notEqual(blackList.head._2)
    for (tup <- blackList) {
      condition =
        condition.and(col(matchTuple(dataset, tup._1)).notEqual(tup._2))
    }
    dataset.filter(condition)
  }

  def applyBlackList(dataset: Dataset[_], blackList: Map[_, _]): Dataset[_] = {
    if (blackList == null || blackList.isEmpty) return dataset
    applyBlackList(dataset, fromMapToList(blackList))
  }

  def applyWhiteList(dataset: Dataset[_],
                     whiteList: List[(_, _)]): Dataset[_] = {
    if (whiteList == null || whiteList.isEmpty) return dataset
    var condition =
      col(matchTuple(dataset, whiteList.head._1)).equalTo(whiteList.head._2)
    for (tup <- whiteList) {
      condition = condition.or(col(matchTuple(dataset, tup._1)).equalTo(tup._2))
    }
    dataset.filter(condition)
  }

  def applyWhiteList(dataset: Dataset[_], whiteList: Map[_, _]): Dataset[_] = {
    if (whiteList == null || whiteList.isEmpty) return dataset
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
          for (elem <- JavaConversions.asScalaBuffer(elems).toList) {
            list = (tup._1, elem) :: list
          }
        case elem => list = (tup._1, elem) :: list
      }
    }
    list
  }
}
