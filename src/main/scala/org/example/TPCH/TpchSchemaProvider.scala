package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}

// TPC-H table schemas
case class Customer_tpch(
                     c_custkey  : Int, //adsf
                     c_name     : String,
                     c_address  : String,
                     c_nationkey: Int,
                     c_phone    : String,
//                     c_acctbal: Double,
//                     c_acctbal: Long,
                     c_acctbal  : Int,
                     c_mktsegment: String,
                     c_comment  : String)

case class Lineitem(
                     l_orderkey: Int,
                     l_partkey: Int,
                     l_suppkey: Int,
                     l_linenumber: Int,
//                     l_quantity: Double,
//                     l_extendedprice: Double,
//                     l_discount: Double,
//                     l_tax: Double,
//                     l_quantity: Long,
//                     l_extendedprice: Long,
//                     l_discount: Long,
//                     l_tax: Long,
                     l_quantity: Int,
                     l_extendedprice: Int,
                     l_discount: Int,
                     l_tax: Int,
//                     l_returnflag: String,
//                     l_linestatus: String,
                     l_returnflag: Int,
                     l_linestatus: Int,
//                     l_shipdate: String,
//                     l_commitdate: String,
//                     l_receiptdate: String,
                     l_shipdate: Int,
                     l_commitdate: Int,
                     l_receiptdate: Int,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)

case class Nation(
                   n_nationkey: Int,
                   n_name: String,
                   n_regionkey: Int,
                   n_comment: String)

case class Order(
                  o_orderkey: Int,
                  o_custkey: Int,
//                  o_orderstatus: String,
                  o_orderstatus: Int,
//                  o_totalprice: Double,
//                  o_totalprice: Long,
                  o_totalprice: Int,
//                  o_orderdate: String,
                  o_orderdate: Int,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Int,
                  o_comment: String)

case class Part(
                 p_partkey: Int,
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Int,
                 p_container: String,
//                 p_retailprice: Double,
//                 p_retailprice: Long,
                 p_retailprice: Int,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Int,
                     ps_suppkey: Int,
                     ps_availqty: Int,
//                     ps_supplycost: Double,
//                     ps_supplycost: Long,
                     ps_supplycost: Int,
                     ps_comment: String)

case class Region(
                   r_regionkey: Int,
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Int,
                     s_name: String,
                     s_address: String,
                     s_nationkey: Int,
                     s_phone: String,
//                     s_acctbal: Double,
//                     s_acctbal: Long,
                     s_acctbal: Int,
                     s_comment: String)

class TpchSchemaProvider(sc: SparkSession, inputDir: String) {
  import sc.implicits._

  val dfMap = Map(
    "customer" -> sc.read.textFile(inputDir + "/customer.tbl*").map(_.split('|')).map(p =>
      Customer_tpch(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, (p(5).trim.toDouble*100).toInt, p(6).trim, p(7).trim)).toDF(),

    "lineitem" -> sc.read.textFile(inputDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, (p(4).trim.toDouble*100).toInt, (p(5).trim.toDouble*100).toInt, (p(6).trim.toDouble*100).toInt, (p(7).trim.toDouble*100).toInt, p(8).trim.indexOf(0).toInt, p(9).trim.indexOf(0).toInt, p(10).trim.replace("-", "").toInt, p(11).trim.replace("-", "").toInt, p(12).trim.replace("-", "").toInt, p(13).trim, p(14).trim, p(15).trim)).toDF(),

    "nation" -> sc.read.textFile(inputDir + "/nation.tbl*").map(_.split('|')).map(p =>
      Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF(),

    "region" -> sc.read.textFile(inputDir + "/region.tbl*").map(_.split('|')).map(p =>
      Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF(),

    "order" -> sc.read.textFile(inputDir + "/orders.tbl*").map(_.split('|')).map(p =>
      Order(p(0).trim.toInt, p(1).trim.toInt, p(2)(0).toInt, (p(3).trim.toDouble*100).toInt, p(4).trim.replace("-", "").toInt, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF(),

    "part" -> sc.read.textFile(inputDir + "/part.tbl*").map(_.split('|')).map(p =>
      Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, (p(7).trim.toDouble*100).toInt, p(8).trim)).toDF(),

    "partsupp" -> sc.read.textFile(inputDir + "/partsupp.tbl*").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, (p(3).trim.toDouble*100).toInt, p(4).trim)).toDF(),

    "supplier" -> sc.read.textFile(inputDir + "/supplier.tbl*").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, (p(5).trim.toDouble*100).toInt, p(6).trim)).toDF()
  )

  // for implicits
  val customer: DataFrame = dfMap("customer")
  val lineitem: DataFrame = dfMap("lineitem")
  val nation: DataFrame = dfMap("nation")
  val region: DataFrame = dfMap("region")
  val order: DataFrame = dfMap("order")
  val part: DataFrame = dfMap("part")
  val partsupp: DataFrame = dfMap("partsupp")
  val supplier: DataFrame = dfMap("supplier")

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}