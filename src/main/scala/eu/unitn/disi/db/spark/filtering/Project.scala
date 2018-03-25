package eu.unitn.disi.db.spark.filtering

import eu.unitn.disi.db.spark.utils.Utils

import org.apache.spark.sql.{DataFrame, Dataset}

object Project {

  def applyProject(dataset: Dataset[_],
                   colsToKeep: java.util.List[_]): DataFrame = {
    Project.applyProject(dataset, Utils.javaToScalaList(colsToKeep))
  }

  def applyProject(dataset: Dataset[_], colsToKeep: List[_]): DataFrame = {
    if (dataset == null || colsToKeep == null || colsToKeep.isEmpty) {
      dataset.toDF()
    } else {
      val columns: List[String] = colsToKeep.map {
        case elem: String => elem.asInstanceOf[String]
        case elem: Int => dataset.columns(elem.asInstanceOf[Int])
      }
      dataset.select(columns.head, columns.tail: _*)
    }
  }
}
