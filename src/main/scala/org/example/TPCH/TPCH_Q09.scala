
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 09
 */
class TPCH_Q09 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select nation, o_year, sum(amount) as sum_profit " +
//      "from (" +
//        "select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount " +
//        "from part, supplier, lineitem, partsupp, orders, nation " +
//        "where s_suppkey = l_suppkey " +
//          "and ps_suppkey = l_suppkey " +
//          "and ps_partkey = l_partkey " +
//          "and p_partkey = l_partkey " +
//          "and o_orderkey = l_orderkey " +
//          "and s_nationkey = n_nationkey " +
//          "and p_name like '%magenta%') as profit " +
//      "group by nation, o_year " +
//      "order by nation, o_year desc")

    sc.sql("select nation, o_year, sum(amount) as sum_profit " +
      "from (" +
        "select n_name as nation, int(o_orderdate/10000) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount " +
        "from part, supplier, lineitem, partsupp, order, nation " +
        "where s_suppkey = l_suppkey " +
          "and ps_suppkey = l_suppkey " +
          "and ps_partkey = l_partkey " +
          "and p_partkey = l_partkey " +
          "and o_orderkey = l_orderkey " +
          "and s_nationkey = n_nationkey " +
          "and p_name like '%magenta%'" +
        ") as profit " +
      "group by nation, o_year " +
      "order by nation, o_year desc")
  }
}
