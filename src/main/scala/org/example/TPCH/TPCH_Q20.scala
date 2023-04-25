
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 20
 */
class TPCH_Q20 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select s_name, s_address " +
//      "from supplier, nation " +
//      "where s_suppkey in " +
//      "(select ps_suppkey " +
//      "from partsupp " +
//      "where ps_partkey in (" +
//      "select p_partkey " +
//      "from part " +
//      "where p_name like 'blush%') " +
//      "and ps_availqty > (" +
//      "select 0.5 * sum(l_quantity) " +
//      "from lineitem " +
//      "where l_partkey = ps_partkey " +
//      "and l_suppkey = ps_suppkey " +
//      "and l_shipdate >= date '1995-01-01' " +
//      "and l_shipdate < date '1995-01-01' + interval '1' year)) " +
//      "and s_nationkey = n_nationkey " +
//      "and n_name = 'INDONESIA' " +
//      "order by s_name")

    sc.sql("select s_name, s_address " +
      "from supplier, nation " +
      "where s_suppkey in " +
        "(select ps_suppkey " +
        "from partsupp " +
        "where ps_partkey in (" +
          "select p_partkey " +
          "from part " +
          "where p_name like 'blush%') " +
          "and ps_availqty > (" +
            "select 0.5 * sum(l_quantity) " +
            "from lineitem " +
            "where l_partkey = ps_partkey " +
            "and l_suppkey = ps_suppkey " +
            "and l_shipdate >= 19950101 " +
            "and l_shipdate < 19960101)) " +
        "and s_nationkey = n_nationkey " +
        "and n_name = 'INDONESIA' " +
      "order by s_name")
  }
}