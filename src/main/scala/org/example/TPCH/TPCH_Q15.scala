
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 15
 */
class TPCH_Q15 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("create view revenue0 (supplier_no,total_revenue) as " +
//      "select l_suppkey, sum(l_extendedprice * (1 - l_discount)) " +
//      "from lineitem " +
//      "where l_shipdate >= date '1996-04-01' " +
//        "and l_shipdate < date '1996-04-01' + interval '3' month " +
//      "group by l_suppkey; " +
//      "select s_suppkey, s_name, s_address, s_phone, total_revenue " +
//      "from supplier, revenue0 " +
//      "where s_suppkey = supplier_no " +
//        "and total_revenue = (select max(total_revenue) from revenue0) " +
//      "order by s_suppkey;")

//    sc.sql("create temp view revenue0 (supplier_no,total_revenue) as " +
//      "select l_suppkey, sum(l_extendedprice * (100 - l_discount)) " +
//      "from lineitem " +
//      "where l_shipdate >= 19960401 " +
//      "and l_shipdate < 19960701 " +
//      "group by l_suppkey; ")
//
//    sc.sql("select s_suppkey, s_name, s_address, s_phone, total_revenue " +
//      "from supplier, revenue0 " +
//      "where s_suppkey = supplier_no " +
//      "and total_revenue = (select max(total_revenue) from revenue0) " +
//      "order by s_suppkey;")

    val q15 ="""
                with revenue (supplier_no, total_revenue) as (
                    select
                      l_suppkey,
                      sum(l_extendedprice * (1-l_discount))
                    from
                      lineitem
                    where
                      l_shipdate >= 1
                      and l_shipdate < (1 + 3)
                    group by
                      l_suppkey
                )

                select
                  s_suppkey,
                  s_name,
                  s_address,
                  s_phone,
                  total_revenue
                from
                  supplier,
                  revenue
                where
                  s_suppkey = supplier_no
                  and total_revenue = (
                    select
                      max(total_revenue)
                    from
                      revenue
                  )
                order by
                  s_suppkey;
        """

//    val q15 = """
//          select
//              s_suppkey,
//              s_name,
//              s_address,
//              s_phone,
//              total_revenue
//          from
//              supplier,
//              (
//                  select
//                      l_suppkey supplier_no,
//                      sum(l_extendedprice * (100 - l_discount)) as total_revenue
//                  from
//                      lineitem
//                  where
//                      l_shipdate >= 19960101
//                      and l_shipdate < 19960401
//                  group by
//                      l_suppkey
//              ) revenue0,
//              (
//                  select
//                      max(total_revenue) m_max
//                  from
//                      (
//                          select
//                              l_suppkey supplier_no,
//                              sum(l_extendedprice * (100 - l_discount)) as total_revenue
//                          from
//                              lineitem
//                          where
//                              l_shipdate >= 19960101
//                              and l_shipdate < 19960401
//                          group by
//                              l_suppkey
//                      ) v1
//              ) v
//          where
//              s_suppkey = supplier_no
//              and total_revenue = v.m_max
//          order by
//              s_suppkey;
//          """

    sc.sql(q15)

  }
}


