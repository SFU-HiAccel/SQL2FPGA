
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 16
 */
class TPCH_Q16 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt " +
//      "from partsupp, part " +
//      "where p_partkey = ps_partkey " +
//      "and p_brand <> 'Brand#32' " +
//      "and p_type not like 'SMALL ANODIZED%' " +
//      "and p_size in (43, 7, 27, 21, 5, 15, 36, 11) " +
//      "and ps_suppkey not in (" +
//        "select s_suppkey " +
//        "from supplier " +
//        "where s_comment like '%Customer%Complaints%'" +
//      ") " +
//      "group by p_brand, p_type, p_size " +
//      "order by supplier_cnt desc, p_brand, p_type, p_size;")

    sc.sql("select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt " +
      "from partsupp, part " +
      "where p_partkey = ps_partkey " +
      "and p_brand <> 'Brand#32' " +
      "and p_type not like 'SMALL ANODIZED%' " +
      "and p_size in (43, 7, 27, 21, 5, 15, 36, 11) " +
      "and ps_suppkey not in (" +
        "select s_suppkey " +
        "from supplier " +
        "where s_comment like '%Customer%Complaints%'" +
      ") " +
      "group by p_brand, p_type, p_size " +
      "order by supplier_cnt desc, p_brand, p_type, p_size;")
  }
}