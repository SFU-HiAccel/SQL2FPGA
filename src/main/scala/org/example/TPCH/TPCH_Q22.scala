
package org.example
import org.apache.spark.sql._

/**
 * TPC-H Query 22
 */
class TPCH_Q22 extends TPCH_Queries {

  override def TPCH_execute(sc: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    // Original Version
//    sc.sql("select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal " +
//      "from (select substring(c_phone from 1 for 2) as cntrycode, c_acctbal " +
//      "from customer " +
//      "where substring(c_phone from 1 for 2) in ('19', '11', '16', '27', '15', '22', '12') " +
//      "and c_acctbal > (select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in ('19', '11', '16', '27', '15', '22', '12')) " +
//      "and not exists (select * from orders where o_custkey = c_custkey)) as custsale " +
//      "group by cntrycode " +
//      "order by cntrycode;")

    sc.sql("select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal " +
      "from (select substring(c_phone from 1 for 2) as cntrycode, c_acctbal " +
        "from customer " +
        "where substring(c_phone from 1 for 2) in ('19', '11', '16', '27', '15', '22', '12') " +
          "and c_acctbal > (select avg(c_acctbal) " +
            "from customer " +
            "where c_acctbal > 0 " +
              "and substring(c_phone from 1 for 2) in ('19', '11', '16', '27', '15', '22', '12')) " +
          "and not exists (select * from order where o_custkey = c_custkey)) as custsale " +
      "group by cntrycode " +
      "order by cntrycode;")
  }
}