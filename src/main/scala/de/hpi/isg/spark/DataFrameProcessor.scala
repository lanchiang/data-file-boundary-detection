package de.hpi.isg.spark

import de.hpi.isg.targets.Line
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2019-07-08
  */
object DataFrameProcessor {

  def mapToLineLengthHistogram(dataFrame: DataFrame): Array[Seq[Double]] = {
    import dataFrame.sparkSession.implicits._
    dataFrame.map(row => valueLengthHistogram(row)).collect()
  }

  /**
    * return a histogram of value length of each field in the row.
    *
    * @param row is the row whose value length in the respective field will be calculated.
    * @return a histogram of value length of each field in the row.
    */
  private def valueLengthHistogram(row: Row): Seq[Double] = {
    row.getValuesMap[Any](row.schema.fieldNames)
            .map(keyVal => Try {
              keyVal._2.toString
            } match {
              case Success(value) => value.length.toDouble
              case Failure(_) => 0
            })
            .toSeq
  }

  def createValueLengthHistogram(lines: Array[Line]): Array[Seq[Double]] = {
    lines.map(valueLengthHistogram)
  }

  private def valueLengthHistogram(line: Line): Seq[Double] = {
    line.cells.map(cell => Try {
      cell.value.toString
    } match {
      case Success(value) => value.length.toDouble
      case Failure(_) => 0
    })
  }

  def fillEmptyLineHistogram(histograms: Array[Seq[Double]]): Array[Seq[Double]] = {
    var lastNonempty = Seq.empty[Double]
    var nonEmptyHistograms = Array.empty[Seq[Double]]
    for (histogram <- histograms) {
      if (histogram.exists(_ != 0)) {
        lastNonempty = histogram
      }
      nonEmptyHistograms = nonEmptyHistograms :+ lastNonempty
    }
    nonEmptyHistograms
  }
}
