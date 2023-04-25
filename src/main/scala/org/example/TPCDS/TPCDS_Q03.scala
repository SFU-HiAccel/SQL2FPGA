package org.example
import org.apache.spark.sql._

/**
 * TPC-DS Query 03
 */
class TPCDS_Q03 extends TPCDS_Queries{

  override def TPCDS_execute(sc: SparkSession, schemaProvider: TpcdsSchemaProvider): DataFrame = {

    //    sc.sql("cache table store_sales;")
    //    sc.sql("cache table household_demographics;")
    //    sc.sql("cache table customer_demographics;")
    //    sc.sql("cache table time_dim;")
    //    sc.sql("cache table store;")
    //    sc.sql("cache table date_dim;")
    //    sc.sql("cache table promotion;")
    //    sc.sql("cache table item;")

    sc.sql("select d_year, item.i_brand_id brand_id, item.i_brand brand, sum(ss_ext_sales_price) sum_agg " +
      "from date_dim, store_sales, item " +
      "where d_date_sk = store_sales.ss_sold_date_sk " +
      "and store_sales.ss_item_sk = item.i_item_sk " +
      "and item.i_manufact_id = 436 " +
      "and d_moy=12 " +
      "group by d_year, item.i_brand, item.i_brand_id " +
      "order by d_year, sum_agg desc, brand_id;")
  }
}