package de.hpi.isg.targets

import de.hpi.isg.utils.CollectionFunctions

/**
  * This class describes a pair of lines in a data file.
  *
  * @author Lan Jiang
  * @since 2019-04-09
  */
class LinePair(val firstLine: Line, val secondLine: Line) {

  lazy val histogramDifference: Double = calculateHistDiff()

  lazy val dataTypeSimilarity: Double = calculateDataTypeSimilarity()

  val linePairIndicator: String = firstLine.lineNumber + "||" + secondLine.lineNumber

  private def calculateHistDiff(): Double = {
    val result = firstLine.cells.zip(secondLine.cells)
            .map(pair => (pair._1.valueLength, pair._2.valueLength))
            .unzip
    CollectionFunctions.histogramDifference(result._1, result._2)
  }

  private def calculateDataTypeSimilarity(): Double = {
    val result = firstLine.cells.zip(secondLine.cells)
            .map(pair => (pair._1.dataType, pair._2.dataType))
    result.map(pair => pair._1 == pair._2).length.toDouble / result.length.toDouble
  }

}
