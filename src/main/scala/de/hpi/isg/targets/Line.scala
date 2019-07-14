package de.hpi.isg.targets

/**
  * This class describes a line in a data file.
  *
  * @author Lan Jiang
  * @since 2019-04-09
  */
class Line(val lineNumber: Int, val content: String, val delimiter: Char) {

  /**
    * All the cells in this line.
    */
  val cells: Array[Cell] = content.split(delimiter).map(new Cell(_))

  def buildHistogram(): Unit = {

  }

}
