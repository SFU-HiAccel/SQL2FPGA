package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 10
 */
class TPCH_Q10 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment " +
//      "from customer, orders, lineitem, nation " +
//      "where c_custkey = o_custkey " +
//      "and l_orderkey = o_orderkey " +
//      "and o_orderdate >= date '1994-08-01' " +
//      "and o_orderdate < date '1994-08-01' + interval '3' month " +
//      "and l_returnflag = 'R' " +
//      "and c_nationkey = n_nationkey " +
//      "group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment " +
//      "order by revenue desc")

    // sc.sql("select c_custkey, c_name, sum(l_extendedprice * (100 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment " +
    // sc.sql("select c_custkey, c_name, sum(l_extendedprice * (100 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone " +
    sc.sql("select c_custkey, c_name, sum(l_extendedprice * (100 - l_discount)) as revenue, c_acctbal, n_name " +
      "from customer, order, lineitem, nation " +
      "where c_custkey = o_custkey " +
      "and l_orderkey = o_orderkey " +
      "and o_orderdate >= 19940801 " +
      "and o_orderdate < 19940801 + 300 " +
      "and l_returnflag = 82 " +
      "and c_nationkey = n_nationkey " +
      // "group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment " +
      // "group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address " +
      "group by c_custkey, c_name, c_acctbal, n_name " +
      "order by revenue desc")
  }
}
