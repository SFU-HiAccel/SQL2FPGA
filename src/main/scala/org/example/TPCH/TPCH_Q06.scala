
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 06
 */
class TPCH_Q06 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    // this is used to implicitly convert an RDD to a DataFrame.
    //    import sc.implicits._

    //original
//    sc.sql("select sum(l_extendedprice * l_discount) as revenue " +
//      "from lineitem " +
//      "where l_shipdate >= date '1994-01-01' " +
//      "and l_shipdate < date '1994-01-01' + interval '1' year " +
//      "and l_discount between .06 - 0.01 and .06 + 0.01 " +
//      "and l_quantity < 24")
    //Modified - date to int, double to int
    sc.sql("select sum(l_extendedprice * l_discount) as revenue " +
      "from lineitem " +
      "where l_shipdate >= 19940101 " +
      "and l_shipdate < 19950101 " +
      "and l_discount between 6 - 1 and 6 + 1 " +
      "and l_quantity < 24")

  }
}
