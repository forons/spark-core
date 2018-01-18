package eu.unitn.disi.db.spark.filtering

import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions

object JavaFilter {
  def applyFilter(dataset: Dataset[_],
                  whiteList: java.util.List[_],
                  blackList: java.util.List[_],
                  colsToKeep: java.util.List[_]): Dataset[_] = {
    val support = Select.applySelect(
      dataset,
      JavaConversions
        .asScalaBuffer(whiteList)
        .toList
        .asInstanceOf[List[(_, _)]],
      JavaConversions.asScalaBuffer(blackList).toList.asInstanceOf[List[(_, _)]]
    )
    Project.applyProject(support,
                         JavaConversions.asScalaBuffer(colsToKeep).toList)
  }
}
