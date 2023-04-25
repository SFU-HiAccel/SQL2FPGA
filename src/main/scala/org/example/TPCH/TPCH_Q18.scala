
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 18
 */
class TPCH_Q18 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) " +
//      "from customer, orders, lineitem " +
//      "where o_orderkey in (" +
//      "select l_orderkey " +
//      "from lineitem " +
//      "group by l_orderkey " +
//      "having sum(l_quantity) > 312" +
//      ") " +
//      "and c_custkey = o_custkey " +
//      "and o_orderkey = l_orderkey " +
//      "group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice " +
//      "order by o_totalprice desc, o_orderdate;")

//    sc.sql("cache table customer;")
//    sc.sql("cache table order;")
//    sc.sql("cache table lineitem;")

    sc.sql("select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) " +
      "from customer, order, lineitem " +
      "where o_orderkey in (" +
          "select l_orderkey " +
          "from lineitem " +
          "group by l_orderkey " +
          "having sum(l_quantity) > 312" +
        ") " +
        "and c_custkey = o_custkey " +
        "and o_orderkey = l_orderkey " +
      "group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice " +
      "order by o_totalprice desc, o_orderdate;")
  }
}