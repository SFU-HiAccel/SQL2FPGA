
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 17
 */
class TPCH_Q17 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select sum(l_extendedprice) / 7.0 as avg_yearly " +
//      "from lineitem, part " +
//      "where p_partkey = l_partkey " +
//      "and p_brand = 'Brand#21' " +
//      "and p_container = 'WRAP BAG' " +
//      "and l_quantity < (" +
//      "select 0.2 * avg(l_quantity) " +
//      "from lineitem " +
//      "where l_partkey = p_partkey" +
//      ");")

    sc.sql("select sum(l_extendedprice) / 7.0 as avg_yearly " +
      "from lineitem, part " +
      "where p_partkey = l_partkey " +
        "and p_brand = 'Brand#21' " +
        "and p_container = 'WRAP BAG' " +
        "and l_quantity < (" +
          "select 0.2 * avg(l_quantity) " +
          "from lineitem " +
          "where l_partkey = p_partkey" +
        ");")
  }
}
