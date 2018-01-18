package eu.unitn.disi.db.spark.filtering

import org.apache.spark.sql.Dataset
import scala.collection.JavaConversions

object JavaSelect {

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.List[_],
                  blackList: java.util.List[_]): Dataset[_] = {
    Select.applySelect(
      dataset,
      JavaConversions
        .asScalaBuffer(whiteList)
        .toList
        .asInstanceOf[List[(_, _)]],
      JavaConversions.asScalaBuffer(blackList).toList.asInstanceOf[List[(_, _)]]
    )
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.Map[_, _],
                  blackList: java.util.Map[_, _]): Dataset[_] = {
    Select.applySelect(dataset,
                       JavaConversions.mapAsScalaMap(whiteList).toMap,
                       JavaConversions.mapAsScalaMap(blackList).toMap)
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.List[_],
                  blackList: java.util.Map[_, _]): Dataset[_] = {
    Select.applySelect(dataset,
                       JavaConversions
                         .asScalaBuffer(whiteList)
                         .toList
                         .asInstanceOf[List[(_, _)]],
                       JavaConversions.mapAsScalaMap(blackList).toMap)
  }

  def applySelect(dataset: Dataset[_],
                  whiteList: java.util.Map[_, _],
                  blackList: java.util.List[_]): Dataset[_] = {
    Select.applySelect(dataset,
                       JavaConversions.mapAsScalaMap(whiteList).toMap,
                       JavaConversions
                         .asScalaBuffer(blackList)
                         .toList
                         .asInstanceOf[List[(_, _)]])
  }

  def applyBlackList(dataset: Dataset[_],
                     blackList: java.util.List[_]): Dataset[_] = {
    Select.applyBlackList(dataset,
                          JavaConversions
                            .asScalaBuffer(blackList)
                            .toList
                            .asInstanceOf[List[(_, _)]])
  }

  def applyBlackList(dataset: Dataset[_],
                     blackList: java.util.Map[_, _]): Dataset[_] = {
    Select.applyBlackList(dataset,
                          JavaConversions.mapAsScalaMap(blackList).toMap)
  }

  def applyWhiteList(dataset: Dataset[_],
                     whiteList: java.util.List[_]): Dataset[_] = {
    Select.applyWhiteList(dataset,
                          JavaConversions
                            .asScalaBuffer(whiteList)
                            .toList
                            .asInstanceOf[List[(_, _)]])
  }

  def applyWhiteList(dataset: Dataset[_],
                     whiteList: java.util.Map[_, _]): Dataset[_] = {
    Select.applyWhiteList(dataset,
                          JavaConversions.mapAsScalaMap(whiteList).toMap)
  }
}
