
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 11
 */
class TPCH_Q11 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select ps_partkey, sum(ps_supplycost * ps_availqty) as value " +
//      "from partsupp, supplier, nation " +
//      "where ps_suppkey = s_suppkey " +
//      "and s_nationkey = n_nationkey " +
//      "and n_name = 'IRAN' " +
//      "group by ps_partkey " +
//        "having sum(ps_supplycost * ps_availqty) > (" +
//          "select sum(ps_supplycost * ps_availqty) * 0.0001000000 " +
//          "from partsupp, supplier, nation " +
//          "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'IRAN') " +
//      "order by value desc")

    sc.sql("select ps_partkey, sum(ps_supplycost * ps_availqty) as value " +
      "from partsupp, supplier, nation " +
      "where ps_suppkey = s_suppkey " +
      "and s_nationkey = n_nationkey " +
      "and n_name = 'IRAN' " +
      "group by ps_partkey " +
      "having sum(ps_supplycost * ps_availqty) > (" +
      "select sum(ps_supplycost * ps_availqty) * 0.0001000000 " +
      "from partsupp, supplier, nation " +
      "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'IRAN') " +
      "order by value desc")
  }
}