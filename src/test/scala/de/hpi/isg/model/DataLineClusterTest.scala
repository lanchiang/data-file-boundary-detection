package de.hpi.isg.model

import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}

import scala.io.Source
import scala.util.Random

/**
  * @author Lan Jiang
  * @since 2019-07-09
  */
class DataLineClusterTest extends FlatSpecLike with Matchers with BeforeAndAfterEach {

  "DataLineCluster" should "" in {
    val filePath = getClass.getClassLoader.getResource("certificates.list").toURI.getPath

    val source = Source.fromFile(filePath)
    val lines = source.getLines().filter(line => !line.equals("")).toSeq
    val sampleLines = Random.shuffle(lines).take((DataLineClusterTest.SAMPLE_PERCENTAGE * lines.length).toInt)

    val cluster = new DataLineCluster
    cluster.run(sampleLines)
  }
}

object DataLineClusterTest {
  private val SAMPLE_PERCENTAGE = 0.01
}

