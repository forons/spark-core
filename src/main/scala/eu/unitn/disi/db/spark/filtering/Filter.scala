package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.utils.Utils

import org.apache.spark.sql.{DataFrame, Dataset}

object Filter {

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: List[(_, _)],
                     blackList: List[(_, _)],
                     colsToKeep: List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: Map[_, _],
                     blackList: Map[_, _],
                     colsToKeep: List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: List[(_, _)],
                     blackList: Map[_, _],
                     colsToKeep: List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: Map[_, _],
                     blackList: List[(_, _)],
                     colsToKeep: List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(dataset, whiteList, blackList)
    Project.applyProject(support, colsToKeep)
  }

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: java.util.List[_],
                     blackList: java.util.List[_],
                     colsToKeep: java.util.List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(
      dataset,
      Utils.javaToScalaTupleList(whiteList),
      Utils.javaToScalaTupleList(blackList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: java.util.Map[_, _],
                     blackList: java.util.List[_],
                     colsToKeep: java.util.List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(
      dataset,
      Utils.javaToScalaMap(whiteList),
      Utils.javaToScalaTupleList(blackList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: java.util.List[_],
                     blackList: java.util.Map[_, _],
                     colsToKeep: java.util.List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(
      dataset,
      Utils.javaToScalaMap(blackList),
      Utils.javaToScalaTupleList(whiteList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }

  def applyFilter[U](dataset: Dataset[U],
                     whiteList: java.util.Map[_, _],
                     blackList: java.util.Map[_, _],
                     colsToKeep: java.util.List[_]): DataFrame = {
    val support: Dataset[U] = Select.applySelect(
      dataset,
      Utils.javaToScalaMap(whiteList),
      Utils.javaToScalaMap(blackList))
    Project.applyProject(support, Utils.javaToScalaList(colsToKeep))
  }
}
