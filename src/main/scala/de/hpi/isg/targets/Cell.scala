package de.hpi.isg.targets

import de.hpi.isg.targets.CellDataType.CellDataType

import scala.util.{Failure, Success, Try}

/**
  * This class represents a cell in a delimitered data line.
  *
  * @author Lan Jiang
  * @since 2019-07-12
  */
class Cell(val value: Any) {

  val valueLength: Double = Try{
    value.toString
  } match {
    case Success(valueString) => valueString.length.toDouble
    case Failure(_) => 0d
  }

  /**
    * The most likely data type of this cell.
    */
  val dataType: CellDataType = detectDataType()

  private def detectDataType(): CellDataType = {
    CellDataType.STRING
  }

}

private object CellDataType extends Enumeration {
  type CellDataType = Value
  val INT, REAL, STRING, DATE, EMPTY = Value
}