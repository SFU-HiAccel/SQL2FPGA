
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 02
 */
class TPCH_Q02 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
    //    import sc.implicits._

    sc.sql("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment " +
      "from part, supplier, partsupp, nation, region " +
      "where p_partkey = ps_partkey " +
      "and s_suppkey = ps_suppkey " +
      "and p_size = 19 " +
      "and p_type like '%NICKEL' " +
      "and s_nationkey = n_nationkey " +
      "and n_regionkey = r_regionkey " +
      "and r_name = 'AMERICA' " +
      "and ps_supplycost = (select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AMERICA') " +
      "order by s_acctbal desc, n_name, s_name, p_partkey;")
  }
}