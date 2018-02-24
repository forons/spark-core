package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.utils.Utils

import org.apache.spark.sql.Dataset

object Project {

  def applyFilter(dataset: Dataset[_],
                  whiteList: java.util.List[_],
                  blackList: java.util.List[_],
                  colsToKeep: java.util.List[_]): Dataset[_] = {
    val support: Dataset[_] = Select.applySelect(
      dataset,
      Utils.javaToScalaTupleList(whiteList),
      Utils.javaToScalaTupleList(blackList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }

  def applyFilter(dataset: Dataset[_],
                  whiteList: java.util.Map[_, _],
                  blackList: java.util.List[_],
                  colsToKeep: java.util.List[_]): Dataset[_] = {
    val support: Dataset[_] = Select.applySelect(
      dataset,
      Utils.javaToScalaMap(whiteList),
      Utils.javaToScalaTupleList(blackList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }

  def applyFilter(dataset: Dataset[_],
                  whiteList: java.util.List[_],
                  blackList: java.util.Map[_, _],
                  colsToKeep: java.util.List[_]): Dataset[_] = {
    val support: Dataset[_] = Select.applySelect(
      dataset,
      Utils.javaToScalaMap(blackList),
      Utils.javaToScalaTupleList(whiteList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }

  def applyFilter(dataset: Dataset[_],
                  whiteList: java.util.Map[_, _],
                  blackList: java.util.Map[_, _],
                  colsToKeep: java.util.List[_]): Dataset[_] = {
    val support: Dataset[_] = Select.applySelect(
      dataset,
      Utils.javaToScalaMap(whiteList),
      Utils.javaToScalaMap(blackList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }

  def applyProject(dataset: Dataset[_],
                   colsToKeep: java.util.List[_]): Dataset[_] = {
    Project.applyProject(dataset, Utils.javaToScalaList(colsToKeep))
  }

  def applyProject(dataset: Dataset[_], colsToKeep: List[_]): Dataset[_] = {
    if (dataset == null || colsToKeep == null || colsToKeep.isEmpty) {
      dataset
    } else {
      val columns: List[String] = colsToKeep.map {
        case elem: String => elem.asInstanceOf[String]
        case elem: Int => dataset.columns(elem.asInstanceOf[Int])
      }
      dataset.select(columns.head, columns.tail: _*)
    }
  }
}
