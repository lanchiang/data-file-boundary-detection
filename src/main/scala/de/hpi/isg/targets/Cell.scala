package de.hpi.isg.targets

import de.hpi.isg.targets.CellDataType.CellDataType

/**
  * This class represents a cell in a delimitered data line.
  *
  * @author Lan Jiang
  * @since 2019-07-12
  */
class Cell(value: Any) {

  /**
    * The most likely data type of this cell.
    */
  private var dataType: CellDataType = detectDataType()

  def detectDataType(): CellDataType = {
    CellDataType.STRING
  }


}

private object CellDataType extends Enumeration {
  type CellDataType = Value
  val INT, REAL, STRING, DATE, EMPTY = Value
}