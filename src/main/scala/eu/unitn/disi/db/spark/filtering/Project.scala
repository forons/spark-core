package eu.unitn.disi.db.spark.filtering

import org.apache.spark.sql.Dataset

object Project {

  def applyProject(dataset: Dataset[_], colsToKeep: List[_]): Dataset[_] = {
    if (dataset == null || colsToKeep == null || colsToKeep.isEmpty)
      return dataset
    val columns: List[String] = colsToKeep.map {
      case elem: String => elem.asInstanceOf[String]
      case elem: Int    => dataset.columns(elem.asInstanceOf[Int])
    }
    dataset.select(columns.head, columns.tail: _*)
  }
}
