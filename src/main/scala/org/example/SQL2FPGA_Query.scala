package org.example
import scala.collection.mutable.ListBuffer

abstract class SQL2FPGA_Query {
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }
  def getName(): String = escapeClassName(this.getClass.getName)
  var goldenOutput = new ListBuffer[String]()
  def setGoldenOutput(input_goldenOutput: ListBuffer[String]) {
    goldenOutput = input_goldenOutput
  }
  def getGoldenOutput(): ListBuffer[String] = {
    return goldenOutput
  }
}
