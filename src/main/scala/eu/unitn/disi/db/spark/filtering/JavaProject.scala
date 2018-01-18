package eu.unitn.disi.db.spark.filtering

import org.apache.spark.sql.Dataset
import scala.collection.JavaConversions

object JavaProject {
  def applyProject(dataset: Dataset[_],
                   colsToKeep: java.util.List[_]): Dataset[_] = {
    Project.applyProject(dataset,
                         JavaConversions.asScalaBuffer(colsToKeep).toList)
  }
}
