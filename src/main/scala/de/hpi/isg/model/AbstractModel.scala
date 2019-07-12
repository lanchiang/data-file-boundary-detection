package de.hpi.isg.model

import org.apache.spark.sql.Row

/**
  * @author Lan Jiang
  * @since 2019-07-12
  */
abstract class AbstractModel {

  abstract def run(row: Row): Unit

}
