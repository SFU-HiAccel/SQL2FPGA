
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 23 - Alec's Testing Query - operator performance model
 */
class TPCH_Q23 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    // Filter
//    sc.sql("select o_orderkey, o_totalprice, o_orderstatus, o_orderdate " +
//      "from order " +
//      "where o_orderdate >= 19930701 ")
//    sc.sql("select l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice " +
//      "from lineitem " +
//      "where l_shipdate <= 19980803 ")
//    sc.sql("select l_orderkey, l_partkey, l_suppkey " +
//      "from lineitem " +
//      "where l_shipdate <= 19980803 ")

    // Inner Join
//    sc.sql("select l_extendedprice,l_quantity " +
//      "from order,lineitem " +
//      "where l_shipdate = o_shippriority " +
//      "and l_orderkey = o_orderkey" +
//    sc.sql("select l_extendedprice,l_quantity " +
//      "from order,lineitem " +
//      "where l_shipdate = o_orderdate " +
//      "and o_orderdate >= 19930701")

//    sc.sql("select l_orderkey, l_suppkey, l_extendedprice, l_discount " +
//          "from part, lineitem " +
//          "where p_partkey = l_partkey")

//    sc.sql("select l_orderkey, l_suppkey, l_extendedprice, l_discount " +
//          "from part, lineitem " +
//          "where p_partkey = l_partkey")

//    sc.sql("SELECT COUNT ( DISTINCT l_partkey ) AS COUNT_STAT " +
//      "FROM lineitem")

//    sc.sql("select p_partkey, count(*) as keyCount " +
//      "from ( select * from part limit 1000 ) " +
//      "group by p_partkey " +
//      "order by p_partkey")

//    sc.sql("select l_partkey, count(*) as keyCount " +
//      "from ( select * from lineitem limit 1000 ) " +
//      "group by l_partkey " +
//      "order by l_partkey")

//    sc.sql("select o_orderkey, o_totalprice, (o_custkey*10) as my_custkey " +
//      "from order " +
//      "where o_orderdate < 19980101 " +
//      "order by o_orderkey asc")

    // Semi Join
//      //   sc.sql("select o_orderpriority,o_custkey,o_orderstatus,o_totalprice " +
//      //   sc.sql("select o_orderpriority,o_custkey " +
//    sc.sql("select o_orderpriority " +
//      "from order " +
//      //      "where exists ( select * from lineitem where l_orderkey = o_orderkey) " )
//      "where exists ( select * from lineitem where l_orderkey = o_orderkey and l_shipdate = o_orderdate)" )

    // Outer Join
//    sc.sql("select o_totalprice,o_orderstatus " +
////    sc.sql("select o_totalprice " +
//      "from order left outer join lineitem on " +
//      "l_orderkey = o_orderkey and l_shipdate = o_orderdate " +
////      "l_orderkey = o_orderkey " +
//      "order by o_totalprice" )

//    // Anti Join
//    sc.sql("select o_totalprice,o_orderstatus " +
//      "from order " +
//      "where o_orderdate not in (" +
//      "select l_shipdate " +
//      "from lineitem ) " +
//      "order by o_totalprice,o_orderstatus")

    // Aggregation
//    sc.sql("select l_returnflag, l_linestatus, " +
//      "sum(l_quantity) as sum_qty, " +
//      "sum(l_extendedprice) as sum_base_price, " +
//      "sum(l_extendedprice * (100 - l_discount)) as sum_disc_price, " +
//      "sum(l_extendedprice * (100 - l_discount) * (100 + l_tax)) as sum_charge, " +
//      "avg(l_quantity) as avg_qty, " +
//      "avg(l_extendedprice) as avg_price, " +
//      "avg(l_discount) as avg_disc " +
//      "from lineitem " +
//      "group by l_returnflag, l_linestatus " +
//      "order by l_returnflag, l_linestatus;")
//    sc.sql("select l_returnflag,l_linestatus " +
////      "sum(l_quantity) as sum_qty, " +
////      "sum(l_extendedprice) as sum_base_price, " +
////      "avg(l_quantity) as avg_qty " +
////      "sum(l_extendedprice * (100 - l_discount)) as sum_disc_price, " +
////      "sum(l_extendedprice * (100 - l_discount) * (100 + l_tax)) as sum_charge " +
//      "from lineitem " +
//      "group by l_returnflag, l_linestatus")


    // HATS Tests
//    // Filter only
//    sc.sql("SELECT l_partkey, l_extendedprice, l_shipdate, l_quantity, l_orderkey, l_receiptdate, l_commitdate, l_suppkey " +
//      "FROM lineitem " +
//      "WHERE l_shipdate <= 19980101")
//    // Evaluate only
//    sc.sql("SELECT l_partkey, l_extendedprice, l_shipdate, l_quantity, (l_quantity * 10) AS _10xqty, l_receiptdate, l_commitdate, l_suppkey " +
//      "FROM lineitem")
    // Filter + Evaluate
    sc.sql("SELECT l_partkey, l_extendedprice, l_shipdate, l_quantity, (l_quantity * 10) AS _10xqty, l_receiptdate, l_commitdate, l_suppkey " +
      "FROM lineitem " +
      "WHERE l_shipdate <= 19980101")
//    // Evaluate + Filter
//    sc.sql("SELECT l_partkey, l_extendedprice, l_shipdate, l_quantity, l_orderkey, l_receiptdate, l_commitdate, l_suppkey " +
//      "FROM lineitem " +
//      "WHERE l_shipdate <= l_orderkey + 19930101")
  }
}