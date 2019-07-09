package de.hpi.isg.model

import smile.clustering.dbscan

/**
  * This cluster groups similar lines of a data file according to the character frequency feature.
  *
  * @author Lan Jiang
  * @since 2019-07-09
  */
class DataLineCluster {

  def run(lines: Seq[String]): Unit = {
    val featureSpace = createFeatureSpace(lines)
    val linePoints = lines.map(line => new LinePoint(line, featureSpace))

    val data = linePoints.map(_.characterVector).map(array => array.map(_._2.toDouble)).toArray
    val clusters = dbscan(data, 10, 5)
  }

  def createFeatureSpace(lines: Seq[String]): Set[Char] = {
    val characterArrayByLine = lines.map(_.toCharArray).zipWithIndex.map(pair => pair.swap).toArray
    val characters = characterArrayByLine
            .flatMap(pair => pair._2)
            .distinct
            .filter(char => char.toInt <= 127)
            .filter(char => !char.isLetterOrDigit)
            .toSet
    characters
  }
}

/**
  * A line point is a vector created by encoding the line into the character space.
  */
private class LinePoint(line: String, featureSpace: Set[Char]) {

  val characterVector: Array[(Char, Int)] = createCharVector()

  private def createCharVector(): Array[(Char, Int)] = {
    val thisVector = line.toCharArray.groupBy(value => value).map(pair => (pair._1, pair._2.length))
    val result = (for (key <- featureSpace) yield (key, thisVector.getOrElse(key, 0))).toArray
    result
  }

}