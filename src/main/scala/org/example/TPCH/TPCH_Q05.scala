package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 05
 */
class TPCH_Q05 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
    //    import sc.implicits._

//    sc.sql("select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue from customer,order,lineitem,supplier,nation,region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and o_orderdate >= date '1993-01-01' and o_orderdate < date '1993-01-01' + interval '1' year group by n_name order by revenue desc");
//    sc.sql("select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue " +
    sc.sql("select n_name,sum(l_extendedprice * (100 - l_discount)) as revenue " +
      "from customer,order,lineitem,supplier,nation,region " +
      "where c_custkey = o_custkey " +
      "and l_orderkey = o_orderkey " +
      "and l_suppkey = s_suppkey " +
      "and c_nationkey = s_nationkey " +
      "and s_nationkey = n_nationkey " +
      "and n_regionkey = r_regionkey " +
      "and r_name = 'AFRICA' " +
      "and o_orderdate >= 19930101 " +
      "and o_orderdate < 19940101 " +
      "group by n_name order by revenue desc")

    /*
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(decrease($"l_extendedprice", $"l_discount")),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")
     */
  }
}
