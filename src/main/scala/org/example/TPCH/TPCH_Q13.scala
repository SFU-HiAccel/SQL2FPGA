
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 13
 */
class TPCH_Q13 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select c_count, count(*) as custdist " +
//      "from (" +
//        "select c_custkey, count(o_orderkey) " +
//          "from customer left outer join orders on " +
//          "c_custkey = o_custkey " +
//          "and o_comment not like '%unusual%deposits%' group by c_custkey" +
//        ") as c_orders (c_custkey, c_count) " +
//      "group by c_count " +
//      "order by custdist desc, c_count desc")

    sc.sql("select c_count, count(*) as custdist " +
      "from (" +
      "select c_custkey, count(o_orderkey) " +
      "from customer left outer join order on " +
      "c_custkey = o_custkey " +
      "and o_comment not like '%unusual%deposits%' group by c_custkey" +
      ") as c_orders (c_custkey, c_count) " +
      "group by c_count " +
      "order by custdist desc, c_count desc")
  }
}