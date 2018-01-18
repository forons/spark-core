package eu.unitn.disi.db.spark.filtering

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

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

  def matchTuple(dataset: Dataset[_], item: Any): String = {
    var column: String = null
    item match {
      case _: Int    => column = dataset.columns(item.asInstanceOf[Int])
      case _: String => column = item.asInstanceOf[String]
      case _         => throw new IllegalArgumentException("This should not happened")
    }
    column
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

  def applyWhiteList(dataset: Dataset[_],
                     whiteList: List[(_, _)]): Dataset[_] = {
    if (whiteList == null || whiteList.isEmpty) return dataset
    var condition =
      col(matchTuple(dataset, whiteList.head._1)).equalTo(whiteList.head._2)
    for (tup <- whiteList) {
      var column: String = null
      tup match {
        case (_: Int, _)    => column = dataset.columns(tup._1.asInstanceOf[Int])
        case (_: String, _) => column = tup._1.asInstanceOf[String]
        case _              => throw new IllegalArgumentException("This should not happened")
      }
      condition = condition.or(col(matchTuple(dataset, tup._1)).equalTo(tup._2))
    }
    dataset.filter(condition)
  }
}
