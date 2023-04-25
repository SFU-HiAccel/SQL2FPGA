package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object SQL2FPGA_Top{
  //SQL2FPGA_QConfig
  var qConfig = new SQL2FPGA_QConfig
  val INPUT_DIR_TPCH  = "/Users/aleclu/dev/tpch-spark/dbgen/tpch_data_01"
  val OUTPUT_DIR_TPCH = "/Users/aleclu/dev/tpch-spark/dbgen/tpch_data_01"
  val INPUT_DIR_TPCDS = "/Users/aleclu/dev/tpcds-spark/dbgen/tpcds_data_1"
  val OUTPUT_DIR_TPCDS = "/Users/aleclu/dev/tpcds-spark/dbgen/tpcds_data_1"
  qConfig.tpch_queryNum_start = 15
  qConfig.tpch_queryNum_end = 22
  qConfig.tpch_queryNum_list = ListBuffer(1) // 3, 13, 15, 18, 20
  qConfig.tpcds_queryNum_start = 1
  qConfig.tpcds_queryNum_end = 22
  qConfig.tpcds_queryNum_list = ListBuffer(1) // 1, 2, 3, 5, 6, 7, 8, 9

  qConfig.pure_sw_mode = 1
  qConfig.scale_factor = 1
  var TPCH_or_DS = 0

  // Macro defines
  val DEBUG_ALL = false
  val DEBUG_PARSER = true | DEBUG_ALL
  val DEBUG_CASCADE_JOIN_OPT = false | DEBUG_ALL
  val DEBUG_STRINGDATATYPE_OPT = false | DEBUG_ALL
  val DEBUG_SPECIAL_JOIN_OPT = false | DEBUG_ALL
  val DEBUG_REDUNDANT_OP_OPT = false | DEBUG_ALL

  //----------------------------------------------------------------------------------------------------------------
  // Table column metadata
  //----------------------------------------------------------------------------------------------------------------
  val columnDictionary = collection.mutable.Map[String, (String, String)]()

  def outputDF(query: SQL2FPGA_Query, df: DataFrame, outputDir: String, elapsed_time: Float): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else {
      var className = query.getName()
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", true).save(outputDir + "/" + className)
      var output_rows = new ListBuffer[String]()
      for (row <- df.collect()) {
        output_rows += row.toString
      }
      output_rows += elapsed_time.toString
      query.setGoldenOutput(output_rows)
    }
  }
  def executeTPCHQueries(sc: SparkSession, schemaProvider: TpchSchemaProvider, qConfig: SQL2FPGA_QConfig, output_dir: String): ListBuffer[(String, Float)] = {
    var codegen = new SQL2FPGA_Codegen
    val results = new ListBuffer[(String, Float)]

    var queryNumList = qConfig.tpch_queryNum_list.toList
    if (qConfig.tpch_queryNum_list.isEmpty) {
      queryNumList = Range(qConfig.tpch_queryNum_start, qConfig.tpch_queryNum_end+1).toList
    }

    // Spark Evaluation Config.
    var sparkEvalResultsFileName = "sparkEvalResults.hpp"
    val outFile = new File(sparkEvalResultsFileName)
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    for (queryNo <- queryNumList) {
      println("Start of SparkSQL Execution")
      val query = Class.forName(f"org.example.TPCH_Q${queryNo}%02d").newInstance().asInstanceOf[TPCH_Queries]
      val df = query.TPCH_execute(sc, schemaProvider)
      for (rep_idx <- 0 until qConfig.num_spark_execution) {
        var tstart_spark = System.nanoTime();
        println("Query" + queryNo)
        df.show(1)
        var tend_spark = System.nanoTime();
        var elapsed_spark_this = (tend_spark - tstart_spark) / 1000000000.0f //second
        println(elapsed_spark_this)
        results += new Tuple2(query.getName(), elapsed_spark_this)
        outputDF(query.asInstanceOf[SQL2FPGA_Query], df, output_dir, elapsed_spark_this)
        codegen.writeSparkSQLEvaluationResults(queryNo, qConfig.scale_factor, elapsed_spark_this, bw)
      }
      println("End of SparkSQL Execution")

      println("Start of SQL2FPGA Compiler")
      println("Query #" + queryNo)
      var num_overlay_orig = 0
      var num_overlay_fused = 0
      //----------------------------------------------------------------------------------------------------------------
      // Spark SQL Parsing: SQL->SparkSQL Execution Plan
      //----------------------------------------------------------------------------------------------------------------
      // Raw SparkSQL Optimized Logical Plan
      println(df.queryExecution.optimizedPlan.toString)

      //----------------------------------------------------------------------------------------------------------------
      // SQL2FPGA Parsing: SparkSQL Execution Plan->SQL2FPGA QPlan
      //----------------------------------------------------------------------------------------------------------------
      // Parsing SparkSQL Optimized Logical Plan
      var qParser = new SQL2FPGA_QParser
      qParser.parse_SparkQPlan_to_SQL2FPGAQPlan_TPCH(df.queryExecution.optimizedPlan, qConfig, schemaProvider)

      //----------------------------------------------------------------------------------------------------------------
      // SQL2FPGA QPlan Optimizations
      //----------------------------------------------------------------------------------------------------------------
      // cascaded-join transformations
      if (queryNo != 9 && queryNo != 10 && queryNo != 4)
        qParser.qPlan.applyCascadedJoinOptTransform(qParser, qConfig, schemaProvider.dfMap)
      // stringDataType transformations
      qParser.qPlan.applyStringDataTypeOptTransform(qParser, qConfig, schemaProvider.dfMap)
      // special-join (outer join + anti join) transformations
      qParser.qPlan.applySpecialJoinOptTransform(qParser, qConfig, schemaProvider.dfMap)
      // redundant operator removal transformation
      qParser.qPlan.applyRedundantNodeRemovalOptTransform(qParser, qConfig, schemaProvider.dfMap)
      // qParser.qPlan.allocateOperators(schemaProvider.dfMap, qConfig.pure_sw_mode)
      // fpga overlay transformations
      val (a,b) = qParser.qPlan.applyFPGAOverlayOptTransform(qParser, qConfig, schemaProvider.dfMap)
      num_overlay_orig = a; num_overlay_fused = b

      //----------------------------------------------------------------------------------------------------------------
      // Code Gen
      //----------------------------------------------------------------------------------------------------------------
      qParser.qPlan.genCode(null, schemaProvider.dfMap, qConfig, queryNo)

      //----------------------------------------------------------------------------------------------------------------
      // Write Execution Code
      //----------------------------------------------------------------------------------------------------------------
      codegen.genHostCode(qParser.qPlan, qConfig.pure_sw_mode, qConfig.num_fpga_device, queryNo, query.getGoldenOutput(), qConfig.scale_factor, num_overlay_orig, num_overlay_fused, 0)
      codegen.genFPGAConfigCode(qParser.qPlan, queryNo, qConfig.scale_factor)
      codegen.genSWConfigCode(qParser.qPlan, queryNo, qConfig.scale_factor)

      println("Query #" + queryNo)
      println("End of SQL2FPGA Compiler")
    }
    bw.close()
    return results

  }
  def executeTPCDSQueries(sc: SparkSession, schemaProvider: TpcdsSchemaProvider, qConfig: SQL2FPGA_QConfig, output_dir: String): ListBuffer[(String, Float)] = {
    var codegen = new SQL2FPGA_Codegen
    val results = new ListBuffer[(String, Float)]

    var queryNumList = qConfig.tpcds_queryNum_list.toList
    if (qConfig.tpcds_queryNum_list.isEmpty) {
      queryNumList = Range(qConfig.tpcds_queryNum_start, qConfig.tpcds_queryNum_end+1).toList
    }

    // Spark Evaluation Config.
    var sparkEvalResultsFileName = "sparkEvalResults.hpp"
    val outFile = new File(sparkEvalResultsFileName)
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    for (queryNo <- queryNumList) {
      println("Start of SparkSQL Execution")
      val query = Class.forName(f"org.example.TPCDS_Q${queryNo}%02d").newInstance().asInstanceOf[TPCDS_Queries]
      val df = query.TPCDS_execute(sc, schemaProvider)
      for (rep_idx <- 0 until qConfig.num_spark_execution) {
        var tstart_spark = System.nanoTime()
        println("Query" + queryNo)
        df.show(1)
        var tend_spark = System.nanoTime();
        var elapsed_spark_this = (tend_spark - tstart_spark) / 1000000000.0f //second
        println(elapsed_spark_this)
        results += new Tuple2(query.getName(), elapsed_spark_this)
        outputDF(query.asInstanceOf[SQL2FPGA_Query], df, output_dir, elapsed_spark_this)
        codegen.writeSparkSQLEvaluationResults(queryNo, qConfig.scale_factor, elapsed_spark_this, bw)
      }
      println("End of SparkSQL Execution")

      println("Start of SQL2FPGA Compiler")
      println("Query #" + queryNo)
      var num_overlay_orig = 0
      var num_overlay_fused = 0
      //----------------------------------------------------------------------------------------------------------------
      // Spark SQL Parsing: SQL->SparkSQL Execution Plan
      //----------------------------------------------------------------------------------------------------------------
      // Raw SparkSQL Optimized Logical Plan
      println(df.queryExecution.optimizedPlan.toString)

      //----------------------------------------------------------------------------------------------------------------
      // SQL2FPGA Parsing: SparkSQL Execution Plan->SQL2FPGA QPlan
      //----------------------------------------------------------------------------------------------------------------
      // Parsing SparkSQL Optimized Logical Plan
      var qParser = new SQL2FPGA_QParser
      qParser.parse_SparkQPlan_to_SQL2FPGAQPlan_TPCDS(df.queryExecution.optimizedPlan, qConfig, schemaProvider)

      //----------------------------------------------------------------------------------------------------------------
      // SQL2FPGA QPlan Optimizations
      //----------------------------------------------------------------------------------------------------------------
      // cascaded-join transformations
      if (queryNo != 9 && queryNo != 10 && queryNo != 4) {
        //        qParser.qPlan.applyCascadedJoinOptTransform(qParser, qConfig, schemaProvider.dfMap)
      }
      // stringDataType transformations
      qParser.qPlan.applyStringDataTypeOptTransform(qParser, qConfig, schemaProvider.dfMap)
      // special-join (outer join + anti join) transformations
      qParser.qPlan.applySpecialJoinOptTransform(qParser, qConfig, schemaProvider.dfMap)
      // redundant operator removal transformation
      qParser.qPlan.applyRedundantNodeRemovalOptTransform(qParser, qConfig, schemaProvider.dfMap)
      // qParser.qPlan.allocateOperators(schemaProvider.dfMap, qConfig.pure_sw_mode)
      // fpga overlay transformations
      val (a,b) = qParser.qPlan.applyFPGAOverlayOptTransform(qParser, qConfig, schemaProvider.dfMap)
      num_overlay_orig = a; num_overlay_fused = b

      //----------------------------------------------------------------------------------------------------------------
      // Code Gen
      //----------------------------------------------------------------------------------------------------------------
      qParser.qPlan.genCode(null, schemaProvider.dfMap, qConfig, queryNo)

      //----------------------------------------------------------------------------------------------------------------
      // Write Execution Code
      //----------------------------------------------------------------------------------------------------------------
      codegen.genHostCode(qParser.qPlan, qConfig.pure_sw_mode, qConfig.num_fpga_device, queryNo, query.getGoldenOutput(), qConfig.scale_factor, num_overlay_orig, num_overlay_fused, 1)
      codegen.genFPGAConfigCode(qParser.qPlan, queryNo, qConfig.scale_factor)
      codegen.genSWConfigCode(qParser.qPlan, queryNo, qConfig.scale_factor)

      println("Query #" + queryNo)
      println("End of SQL2FPGA Compiler")
    }
    bw.close()
    return results
  }

  def main(args: Array[String]): Unit = {
    // Start Spark Session
    var t0_spark = System.nanoTime()
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SQL2FPGA Query Demo")
      .getOrCreate()

    var t1_spark = System.nanoTime();
    val elapsed_spark_builder = (t1_spark - t0_spark) / 1000000000.0f //second
    println("Spark builder time: " + elapsed_spark_builder)

    // Start SQL2FPGA Compilation
    val output = new ListBuffer[(String, Float)]
    if (TPCH_or_DS == 0) {
      val tpchschemaProvider  = new TpchSchemaProvider(spark, INPUT_DIR_TPCH);
      output ++= executeTPCHQueries(spark, tpchschemaProvider, qConfig, OUTPUT_DIR_TPCH);
    } else if (TPCH_or_DS == 1) {
      val tpcdsschemaProvider = new TpcdsSchemaProvider(spark, INPUT_DIR_TPCDS);
      output ++= executeTPCDSQueries(spark, tpcdsschemaProvider, qConfig, OUTPUT_DIR_TPCDS);
    }

    // Write Query Results
    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))
    output.foreach {
      case (key, value) =>bw.write(f"${key}%s\t${value}%1.8f\n")
    }
    bw.close()
  }

}