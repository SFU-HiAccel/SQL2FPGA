package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class TPCH_Queries extends SQL2FPGA_Query {
  def TPCH_execute(sc: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

