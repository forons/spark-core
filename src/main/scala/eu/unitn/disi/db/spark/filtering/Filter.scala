package eu.unitn.disi.db.spark.filtering

import org.apache.spark.sql.Dataset

object Filter {

  def applyFilter(dataset: Dataset[_],
                  whiteList: List[(_, _)],
                  blackList: List[(_, _)],
                  colsToKeep: List[_]): Dataset[_] = {
    val support = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }

  def applyFilter(dataset: Dataset[_],
                  whiteList: Map[_, _],
                  blackList: Map[_, _],
                  colsToKeep: List[_]): Dataset[_] = {
    val support = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }

  def applyFilter(dataset: Dataset[_],
                  whiteList: List[(_, _)],
                  blackList: Map[_, _],
                  colsToKeep: List[_]): Dataset[_] = {
    val support = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }

  def applyFilter(dataset: Dataset[_],
                  whiteList: Map[_, _],
                  blackList: List[(_, _)],
                  colsToKeep: List[_]): Dataset[_] = {
    val support = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }
}
