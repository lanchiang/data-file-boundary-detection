package de.hpi.isg.targets

/**
  * This class describes a line in a data file.
  *
  * @author Lan Jiang
  * @since 2019-04-09
  */
class Line(val content: String, val lineNumber: Int) {

  /**
    * All the cells in this line.
    */
  private var cells: Array[Cell] = _

  def buildHistogram(): Unit = {

  }

}
