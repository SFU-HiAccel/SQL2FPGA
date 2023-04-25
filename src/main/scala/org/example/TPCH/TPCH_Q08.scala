
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 08
 */
class TPCH_Q08 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

      // Original Version
//    sc.sql("select o_year, sum(case when nation = 'IRAQ' then volume else 0 end) / sum(volume) as mkt_share " +
//      "from (" +
//        "select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation " +
//        "from part, supplier, lineitem, orders, customer, nation n1, nation n2, region " +
//        "where p_partkey = l_partkey " +
//          "and s_suppkey = l_suppkey " +
//          "and l_orderkey = o_orderkey " +
//          "and o_custkey = c_custkey " +
//          "and c_nationkey = n1.n_nationkey " +
//          "and n1.n_regionkey = r_regionkey " +
//          "and r_name = 'MIDDLE EAST' " +
//          "and s_nationkey = n2.n_nationkey " +
//          "and o_orderdate between date '1995-01-01' and date '1996-12-31' " +
//          "and p_type = 'LARGE ANODIZED STEEL'" +
//      ") as all_nations " +
//      "group by o_year " +
//      "order by o_year")

    sc.sql("select o_year, sum(case when nation = 'IRAQ' then volume else 0 end) / sum(volume) as mkt_share " +
      "from (" +
      "select int(o_orderdate/10000) as o_year, l_extendedprice * (100 - l_discount) as volume, n2.n_name as nation " +
      "from part, supplier, lineitem, order, customer, nation n1, nation n2, region " +
      "where p_partkey = l_partkey " +
      "and s_suppkey = l_suppkey " +
      "and l_orderkey = o_orderkey " +
      "and o_custkey = c_custkey " +
      "and c_nationkey = n1.n_nationkey " +
      "and n1.n_regionkey = r_regionkey " +
      "and r_name = 'MIDDLE EAST' " +
      "and s_nationkey = n2.n_nationkey " +
      "and o_orderdate between 19950101 and 19961231 " +
      "and p_type = 'LARGE ANODIZED STEEL'" +
      ") as all_nations " +
      "group by o_year " +
      "order by o_year")
  }
}
