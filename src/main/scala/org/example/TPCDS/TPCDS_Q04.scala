package org.example
import org.apache.spark.sql._

/**
 * TPC-DS Query 04
 */
class TPCDS_Q04 extends TPCDS_Queries{

  override def TPCDS_execute(sc: SparkSession, schemaProvider: TpcdsSchemaProvider): DataFrame = {

    sc.sql("cache table store_sales;")
    sc.sql("cache table household_demographics;")
    sc.sql("cache table customer_demographics;")
    sc.sql("cache table time_dim;")
    sc.sql("cache table store;")
    sc.sql("cache table date_dim;")
    sc.sql("cache table promotion;")
    sc.sql("cache table item;")


    sc.sql("select sum(cs_ext_discount_amt)  as excess " +
      "from    catalog_sales    ,item    ,date_dim " +
      "where i_manufact_id = 291 and i_item_sk = cs_item_sk and d_date between 20000322 and 20000622 and d_date_sk = cs_sold_date_sk and cs_ext_discount_amt      > (          select             1.3 * avg(cs_ext_discount_amt)          from             catalog_sales            ,date_dim          where               cs_item_sk = i_item_sk           and d_date between 20000322 and 20000622           and d_date_sk = cs_sold_date_sk       );"
    )
  }
}