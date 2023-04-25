package org.example
import org.apache.spark.sql._

/**
 * TPC-DS Query 01
 */
class TPCDS_Q01 extends TPCDS_Queries{

  override def TPCDS_execute(sc: SparkSession, schemaProvider: TpcdsSchemaProvider): DataFrame = {
//    sc.sql("cache table store_sales;")
//    sc.sql("cache table household_demographics;")
//    sc.sql("cache table customer_demographics;")
//    sc.sql("cache table time_dim;")
//    sc.sql("cache table store;")
//    sc.sql("cache table date_dim;")
//    sc.sql("cache table promotion;")
//    sc.sql("cache table item;")
//    sc.sql("cache table catalog_sales;")

//    sc.sql("select count(*) " +
//      "from store_sales, household_demographics, time_dim, store " +
//      "where ss_sold_time_sk = time_dim.t_time_sk" +
//      "     and ss_hdemo_sk = household_demographics.hd_demo_sk" +
//      "     and ss_store_sk = s_store_sk" +
//      "     and time_dim.t_hour = 8" +
//      "     and time_dim.t_minute >= 30" +
//      "     and household_demographics.hd_dep_count = 5" +
//      "     and store.s_store_name = 'ese'" +
//      " order by count(*)")

    var tpcds_q1 = """ WITH customer_total_return
                     |     AS (SELECT sr_customer_sk     AS ctr_customer_sk,
                     |                sr_store_sk        AS ctr_store_sk,
                     |                Sum(sr_return_amt) AS ctr_total_return
                     |         FROM   store_returns,
                     |                date_dim
                     |         WHERE  sr_returned_date_sk = d_date_sk
                     |                AND d_year = 2001
                     |         GROUP  BY sr_customer_sk,
                     |                   sr_store_sk)
                     |SELECT c_customer_id
                     |FROM   customer_total_return ctr1,
                     |       store,
                     |       customer
                     |WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2
                     |                                FROM   customer_total_return ctr2
                     |                                WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk)
                     |       AND s_store_sk = ctr1.ctr_store_sk
                     |       AND s_state = 'TN'
                     |       AND ctr1.ctr_customer_sk = c_customer_sk
                     |ORDER  BY c_customer_id """.stripMargin
    sc.sql(tpcds_q1)
  }
}