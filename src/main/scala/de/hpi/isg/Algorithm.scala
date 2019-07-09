package de.hpi.isg

import de.hpi.isg.spark.{DataFileReader, DataFrameProcessor}
import de.hpi.isg.utils.SeqFunctions
import de.hpi.isg.utils.SeqFunctions.HISTOGRAM_ALGORITHM

/**
  *
  * The data file boundary detection algorithm class.
  *
  * @author Lan Jiang
  * @since 2019-07-08
  */
class Algorithm(val config: String) {

  /**
    * Execute the algorithm.
    */
  def run(): Unit = {
    detectDelimiterCandidates()
            .map(delimiter => new DataFileReader(String.valueOf(delimiter)).readFile())
            .map(dataset => DataFrameProcessor.mapToLineLengthHistogram(dataset))
            .map(histograms => DataFrameProcessor.fillEmptyLineHistogram(histograms))
            .map(histograms => {
              // calculate the histogram difference of the neighbouring pairs of histograms, zipping them with index
              val histogramDifference = histograms
                      .sliding(2)
                      .map(pair =>
                        SeqFunctions.histogramDifference(pair(0), pair(1), HISTOGRAM_ALGORITHM.Bhattacharyya)
                      )
                      .toSeq
              val (min, max) = (histogramDifference.min, histogramDifference.max)
              val histogramDiff = zipWithLinePairIndicatorOrdered(histogramDifference, min, max)
              histogramDiff

              val knee = detectKnee(histogramDiff)
            })
  }

  def detectDelimiterCandidates(): Seq[Char] = {
    ???
  }

  /**
    * Create a line pair indicator in the form of "L1||L2" for each of the histogram difference. "L1||L2" means this histogram difference is between
    * the line L1 and L2.
    *
    * @param histogramDifference
    * @param min
    * @param max
    * @return
    */
  private def zipWithLinePairIndicator(histogramDifference: Seq[Double], min: Double, max: Double): Seq[(String, Double)] = {
    histogramDifference
            .map(diff => (diff-min)/(max-min))
            .zipWithIndex
            .map(pair => {
              val x = (pair._2+1).toString.concat("||").concat((pair._2+2).toString)
              val y = pair._1
              (x,y)
            })
  }

  /**
    * Create a line pair indicator in the form of "L1||L2" for each of the histogram difference, and then order the sequence by the histogram difference score.
    * "L1||L2" means this histogram difference is between the line L1 and L2.
    *
    * @param histogramDifference
    * @param min
    * @param max
    * @return
    */
  private def zipWithLinePairIndicatorOrdered(histogramDifference: Seq[Double], min: Double, max: Double): Seq[(String, Double)] = {
    zipWithLinePairIndicator(histogramDifference, min, max).sortBy(indexedHistDiff => indexedHistDiff._2)(Ordering[Double].reverse)
  }

  /**
    * Detect the knee in the given ordered histogram difference sequence. The returned knee is composed of two parts: the line pair indicator and the biggest histogram difference.
    *
    * @param histogramDifferenceOrdered the ordered histogram difference
    * @return the knee point
    */
  def detectKnee(histogramDifferenceOrdered: Seq[(String, Double)]): (String, Double) = {
    histogramDifferenceOrdered
            .sliding(2)
            .map(pair => (pair(0)._1 , pair(0)._2 - pair(1)._2))
            .maxBy(_._2)
  }
}
