
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 03
 */
class TPCH_Q03 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
    //    import sc.implicits._

//    sc.sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " +
//      "from customer, orders, lineitem " +
//      "where c_mktsegment = 'MACHINERY' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-07' and l_shipdate > date '1995-03-07' " +
//      "group by l_orderkey, o_orderdate, o_shippriority " +
//      "order by revenue desc, o_orderdate;")

//    sc.conf.set("spark.sql.inMemoryColumnarStorage.compressed",false)
//    sc.conf.set("spark.sql.adaptive.enabled",false)
//    sc.conf.set("spark.sql.adaptive.coalescePartitions.enabled",false)
//    sc.conf.set("spark.sql.adaptive.skewJoin.enabled",false)
//    sc.conf.set("spark.sql.cbo.enabled",false)
//    sc.conf.set("spark.sql.adaptive.coalescePartitions.enabled",false)

    sc.sql("select l_orderkey, sum(l_extendedprice * (100 - l_discount)) as revenue, o_orderdate, o_shippriority " +
      "from customer, order, lineitem " +
      "where c_mktsegment = 'MACHINERY' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < 19950307 and l_shipdate > 19950307 " +
      "group by l_orderkey, o_orderdate, o_shippriority " +
      "order by revenue desc, o_orderdate;")
  }
}