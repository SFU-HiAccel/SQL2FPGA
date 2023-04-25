package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class TPCDS_Queries extends SQL2FPGA_Query {
  def TPCDS_execute(sc: SparkSession, tpcdsSchemaProvider: TpcdsSchemaProvider): DataFrame
}

