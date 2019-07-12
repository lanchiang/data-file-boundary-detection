package de.hpi.isg.model

import smile.clustering.{DBSCAN, dbscan}

/**
  * This cluster groups similar lines of a data file according to the character frequency feature.
  *
  * @author Lan Jiang
  * @since 2019-07-09
  */
class DataLineCluster {

  private var clusters: DBSCAN[Array[Double]] = _

  private var linePoints: Seq[LinePoint] = _

  def run(lines: Seq[String]): Unit = {
    val featureSpace = createFeatureSpace(lines)
    linePoints = lines.map(line => new LinePoint(line, featureSpace))

    val data = linePoints.map(_.characterVector).map(array => array.map(_._2.toDouble)).toArray
    clusters = dbscan(data, 100, 1)
  }

  def getGroups: Map[Int, Array[LinePoint]] = {
    val labeledLinePoints = linePoints.toArray.zip(clusters.getClusterLabel)
    val groupedLinePointsByLabel = labeledLinePoints.groupBy(_._2).map(pair => (pair._1, pair._2.map(labeledLinePoint => labeledLinePoint._1)))
    groupedLinePointsByLabel
  }

  private def createFeatureSpace(lines: Seq[String]): Set[Char] = {
    val characterArrayByLine = lines.map(_.toCharArray).zipWithIndex.map(pair => pair.swap).toArray
    characterArrayByLine
            .flatMap(pair => pair._2)
            .distinct
            .filter(char => char.toInt <= 127)
            .filter(char => !char.isLetterOrDigit)
            .toSet
  }
}

/**
  * A line point is a vector created by encoding the line into the character space.
  */
protected class LinePoint(val line: String, val featureSpace: Set[Char]) {

  val characterVector: Array[(Char, Int)] = createCharVector()

  private def createCharVector(): Array[(Char, Int)] = {
    val thisVector = line.toCharArray.groupBy(value => value).map(pair => (pair._1, pair._2.length))
    val result = (for (key <- featureSpace) yield (key, thisVector.getOrElse(key, 0))).toArray
    result
  }

}