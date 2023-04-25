package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 04
 */
class TPCH_Q04 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
    //    import sc.implicits._

    sc.sql("select o_orderpriority, count(*) as order_count " +
      "from order " +
      "where o_orderdate >= 19930701 " +
      "and o_orderdate < 19931001 " +
      "and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) " +
      "group by o_orderpriority " +
      "order by o_orderpriority")

  }
}
