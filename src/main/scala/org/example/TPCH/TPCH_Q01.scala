package org.example
import org.apache.spark.sql._


/**
 * TPC-H Query 01
 */
class TPCH_Q01 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
//    sc.sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order " +
//      "from lineitem " +
//      "where l_shipdate <= date '1998-12-01' - interval '120' day " +
//      "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;")

//    sc.sql("cache table customer;")
//    sc.sql("cache table lineitem;")
//    sc.sql("cache table nation;")
//    sc.sql("cache table region;")
//    sc.sql("cache table order;")
//    sc.sql("cache table part;")
//    sc.sql("cache table partsupp;")
//    sc.sql("cache table supplier;")

    sc.sql("select l_returnflag, l_linestatus, " +
        "sum(l_quantity) as sum_qty, " +
        "sum(l_extendedprice) as sum_base_price, " +
        "sum(l_extendedprice * (100 - l_discount)) as sum_disc_price, " +
        "sum(l_extendedprice * (100 - l_discount) * (100 + l_tax)) as sum_charge, " +
        "avg(l_quantity) as avg_qty, " +
        "avg(l_extendedprice) as avg_price, " +
        "avg(l_discount) as avg_disc, " +
        "count(*) as count_order " +
      "from lineitem " +
      "where l_shipdate <= 19980803 " +
      "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;")
  }
}