
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 12
 */
class TPCH_Q12 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select l_shipmode, " +
//      "sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, " +
//      "sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count " +
//      "from orders, lineitem " +
//      "where o_orderkey = l_orderkey " +
//        "and l_shipmode in ('TRUCK', 'MAIL') " +
//        "and l_commitdate < l_receiptdate " +
//        "and l_shipdate < l_commitdate " +
//        "and l_receiptdate >= date '1997-01-01' " +
//        "and l_receiptdate < date '1997-01-01' + interval '1' year " +
//      "group by l_shipmode " +
//      "order by l_shipmode")

    sc.sql("select l_shipmode, " +
      "sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, " +
      "sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count " +
      "from order, lineitem " +
      "where o_orderkey = l_orderkey " +
        "and l_shipmode in ('TRUCK', 'MAIL') " +
        "and l_commitdate < l_receiptdate " +
        "and l_shipdate < l_commitdate " +
        "and l_receiptdate >= 19970101 " +
        "and l_receiptdate < 19980101 " +
      "group by l_shipmode " +
      "order by l_shipmode")
  }
}