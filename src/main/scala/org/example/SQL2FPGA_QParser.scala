package org.example
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendColumns, AppendColumnsWithObject, BinaryNode, DeserializeToObject, Distinct, Filter, Generate, GlobalLimit, Join, LocalLimit, LogicalPlan, MapElements, Project, Repartition, RepartitionByExpression, Sample, SerializeFromObject, Sort, SubqueryAlias, TypedFilter, UnaryNode, Window}
import org.example.SQL2FPGA_Top.{DEBUG_PARSER, columnDictionary}

import scala.collection.mutable.ListBuffer

//----------------------------------------------------------------------------------------------------------------
// SQL parsing functions: Spark AST -> SQL2FPGA AST
//----------------------------------------------------------------------------------------------------------------
class SQL2FPGA_QParser {
  //------------------------
  // definitions
  //------------------------
  private var _qPlan: SQL2FPGA_QPlan = new SQL2FPGA_QPlan
  private var _qPlan_backup: SQL2FPGA_QPlan = new SQL2FPGA_QPlan
  //------------------------
  // getters
  //------------------------
  def qPlan = _qPlan
  def qPlan_backup = _qPlan_backup
  //------------------------
  // setters
  //------------------------
  def qPlan_= (newValue: SQL2FPGA_QPlan): Unit = {
    _qPlan = newValue
  }
  def qPlan_backup_= (newValue: SQL2FPGA_QPlan): Unit = {
    _qPlan_backup = newValue
  }
  //------------------------
  // functions
  //------------------------
  def getTableRow(tbl_name: String, scale_factor: Int): Int = {
    var result = 0
    tbl_name match {
      case "lineitem" =>
        result = 6001215 // 6001215*30 = 180,036,450 -> div by 32 => 5,626,140
      case "order" =>
        result = 1500000 // 1500000*30 = 45,000,000
      case "partsupp" =>
        result = 800000 // 800000*30 = 24,000,000
      case "part" =>
        result = 200000 // 200000*30 = 6,000,000
      case "customer" =>
        result = 150000 // 150000*30 = 4,500,000
      case "supplier" =>
        result = 10000 // 10000*30 = 300,000
      case "nation" =>
        result = 25 // 25*30 = 750
      case "region" =>
        result = 5 // 5*30 = 150
      case _ =>
        result = -1
    }
    if (result != -1) {
      result = scale_factor * result
    }
    return result
  }

  def print_indent(num_indent: Int): Unit = {
    for (it <- 0 until (num_indent)) {
      print("\t")
    }
  }

  def printUnaryOperation(u: UnaryNode,
                          num_indent: Int,
                          root_required_col: ListBuffer[String],
                          fpga_plan: SQL2FPGA_QPlan,
                          sf: Int): Unit = {
    print(num_indent.toString)
    var nodeType = ""
//    for (ptrn <- u.nodePatterns) {
//      print_indent(num_indent)
//      println(ptrn.toString)
//      nodeType = nodeType.concat(ptrn.toString)
//    }
    nodeType = u.nodeName
    fpga_plan.nodeType = nodeType
    var validOp = true
    var isProject = false
    var parent_required_col = new ListBuffer[String]()

    u match {
      case al_sort: Sort =>
        print_indent(num_indent)
        print("Output: ")
        var outputCols = new ListBuffer[String]()
        for (_output <- al_sort.output) {
          print(_output.toString + ", ")
          outputCols += _output.toString
        }
        fpga_plan.outputCols = outputCols
        print("\n")
        print_indent(num_indent)
        print("Process: ")
        var operation = new ListBuffer[String]()
        var sorting_expression = new ListBuffer[Expression]()
        for (_order <- al_sort.order) {
          print(_order.toString + ", ")
          operation += _order.toString
          sorting_expression += _order
        }
        fpga_plan.operation = operation
        fpga_plan.sorting_expression = sorting_expression
        print("\n")
        print_indent(num_indent)
        print("Input: ")
        println(al_sort.inputSet)
      case al_aggr: Aggregate =>
        print_indent(num_indent)
        print("Output: ")
        var outputCols = new ListBuffer[String]()
        for (_output <- al_aggr.output) {
          print(_output.toString + ", ")
          outputCols += _output.toString
        }
        fpga_plan.outputCols = outputCols
        print("\n")
        print_indent(num_indent)
        print("Process: ")
        var operation = new ListBuffer[String]()
        var aggregate_operation = new ListBuffer[String]()
        var groupBy_operation = new ListBuffer[String]()
        var aggregate_expression = new ListBuffer[Expression]()
        var groupBy_expression = new ListBuffer[Expression]()
        for (_expr <- al_aggr.groupingExpressions) {
          if (!operation.contains(_expr.toString)) {
            print(_expr.toString + ", ")
            operation += _expr.toString
            groupBy_operation += _expr.toString
            groupBy_expression += _expr
          }
        }
        for (_expr <- al_aggr.aggregateExpressions) {
          if (_expr.getClass.getName != "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
            print(_expr.toString + ", ")
            operation += _expr.toString
            aggregate_operation += _expr.toString
            aggregate_expression += _expr
          }
        }
        fpga_plan.operation = operation
        fpga_plan.aggregate_operation = aggregate_operation
        fpga_plan.groupBy_operation = groupBy_operation
        fpga_plan.aggregate_expression = aggregate_expression
        fpga_plan.groupBy_expression = groupBy_expression
        print("\n")
        print_indent(num_indent)
        print("Input: ")
        println(al_aggr.inputSet)
      case al_filter: Filter =>
        print_indent(num_indent)
        print("Output: ")
        for (root_col <- root_required_col) {
          if (!parent_required_col.contains(root_col)) {
            parent_required_col += root_col
          }
        }
        var temp_output_required_col = new ListBuffer[String]()
        for (_output <- parent_required_col) {
          print(_output + ", ")
          temp_output_required_col += _output
        }
        fpga_plan.outputCols = temp_output_required_col
        print("\n")
        print_indent(num_indent)
        print("Process: ")
        print(al_filter.condition)
        fpga_plan.filtering_expression = al_filter.condition
        var operation = new ListBuffer[String]()
        operation += al_filter.condition.toString
        fpga_plan.operation = operation
        print("\n")
        print_indent(num_indent)
        print("Input: ")
        for (_input <- parent_required_col) {
          print(_input + ", ")
        }
        for (_reference <- al_filter.references) {
          if (!parent_required_col.contains(_reference.toString)) {
            parent_required_col += _reference.toString
            print(_reference.toString + ", ")
          }
        }
        print("\n")
      case al_proj: Project =>
        var pure_project = true
        // Check in ProjecList to see if any aggregation is done
        for (item <- al_proj.projectList.toList) {
          if (!al_proj.outputSet.toList.contains(item)) {
            pure_project = false
          }
        }
        if (pure_project == true) { // simple projection
          print_indent(num_indent)
          print("Output: ")
          for (_output <- al_proj.output) {
            print(_output.toString + ", ")
            if (!parent_required_col.contains(_output.toString)) {
              parent_required_col += _output.toString
            }
          }
          fpga_plan.outputCols = parent_required_col
          print("\n")
          isProject = true
        }
        else { // aggregation is needed
          print_indent(num_indent)
          print("Output: ")
          var outputCols = new ListBuffer[String]()
          for (_output <- al_proj.output) {
            print(_output.toString + ", ")
            outputCols += _output.toString
          }
          fpga_plan.outputCols = outputCols
          print("\n")
          print_indent(num_indent)
          print("Process: ")
          var operation = new ListBuffer[String]()
          var aggregate_operation = new ListBuffer[String]()
          var groupBy_operation = new ListBuffer[String]()
          var aggregate_expression = new ListBuffer[Expression]()
          for (item <- al_proj.projectList.toList) {
            //aggregate columns
            if (!al_proj.outputSet.toList.contains(item)) {
              print(item.toString + ", ")
              print(" || " + item.getClass + " || ")
              operation += item.toString
              aggregate_operation += item.toString
              aggregate_expression += item
            }
          }
          fpga_plan.operation = operation
          fpga_plan.aggregate_operation = aggregate_operation
          fpga_plan.groupBy_operation = groupBy_operation
          fpga_plan.aggregate_expression = aggregate_expression
          print("\n")
          print_indent(num_indent)
          print("Input: ")
          for (item <- al_proj.references) {
            if (!parent_required_col.contains(item.toString)) {
              parent_required_col += item.toString
            }
            print(item + ", ")
          }
          print("\n")
          println("this project node will be treated as an aggregate_proj node")
          //          fpga_plan.nodeType = "AGGREGATE"
        }
      case _: SubqueryAlias => println(" SubqueryAlias ")
      case _: Window => println(" Window ")
      case _: Sample => println(" Sample ")
      case _: GlobalLimit => println(" GlobalLimit ")
      case _: LocalLimit => println(" LocalLimit ")
      case _: Generate => println(" Generate ")
      case _: Distinct => println(" Distinct ")
      case _: AppendColumns => println(" AppendColumns ")
      case _: AppendColumnsWithObject => println(" AppendColumnsWithObject ")
      case _: RepartitionByExpression => println(" RepartitionByExpression ")
      case _: Repartition => println(" Repartition ")
      case _: TypedFilter => println(" TypedFilter ")
      case al_serial: SerializeFromObject =>
        var tcph_table: String = ""
        for (root_col <- root_required_col) {
          if (!parent_required_col.contains(root_col)) {
            parent_required_col += root_col
          }
        }
        print_indent(num_indent)
        print("Output: ")
        // for (_output <- al_serial.outputSet) { print(_output.toString + ", ") }
        for (_output <- parent_required_col) {
          print(_output + ", ")
        }
        fpga_plan.outputCols = parent_required_col
        fpga_plan.inputCols = parent_required_col
        print("\n")
        print_indent(num_indent)
        print("Process: ")
        for (_attr <- al_serial.references) {
          var table: String = _attr.dataType.catalogString
          val tbl_last = table.split("\\.").last
          print(" " + tbl_last + ": ")
          if (tbl_last.split('_').head.toLowerCase() == "customer" &&
            (tbl_last.split('_').last.toLowerCase() == "tpcds"|| tbl_last.split('_').last.toLowerCase() == "tpch")) {
            tcph_table = tbl_last.split('_').head.toLowerCase() // get "customer" from "customer_tpch/tpcds"
          } else {
            tcph_table = tbl_last.toLowerCase()
          }
        }
        for (_column <- parent_required_col) {
          var col: String = _column
          val col_first = col.split("#").head
          print(col_first + ", ")
          columnDictionary += (col -> (tcph_table, col_first))
        }
        fpga_plan.numTableRow = getTableRow(tcph_table, sf)
        print("\n")
        validOp = false
      case _: DeserializeToObject => println(" DeSerializeToObject ")
      case _: MapElements => println(" MapElements ")
      case al_other => println(al_other.getClass.getName)
    }
    var typeString_1 = u.getClass.getName
    var typeString_2 = "org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject"
    if (typeString_1 != typeString_2) {
      for (_i_col <- u.inputSet) {
        print(_i_col.toString + ":" + _i_col.name + ":" + _i_col.dataType + " \n")
        if (!columnDictionary.contains(_i_col.toString)) {
          columnDictionary += (_i_col.toString -> (_i_col.dataType.toString, "NULL"))
        }
      }
      for (_o_col <- u.outputSet) {
        print(_o_col.toString + ":" + _o_col.name + ":" + _o_col.dataType + " \n")
        if (!columnDictionary.contains(_o_col.toString)) {
          columnDictionary += (_o_col.toString -> (_o_col.dataType.toString, "NULL"))
        }
      }
    }
    if (validOp == true) {
      var allChildren = u.children ++ u.innerChildren
      for (ch <- allChildren) {
        if (isProject == true) {
          parse_optimized_query(ch, num_indent, parent_required_col, fpga_plan, sf)
        } else {
          var next_fpga_plan = new SQL2FPGA_QPlan
          next_fpga_plan.treeDepth = fpga_plan.treeDepth + 1
          parse_optimized_query(ch, num_indent + 1, parent_required_col, next_fpga_plan, sf)
          var current_children = fpga_plan.children
          current_children += next_fpga_plan
          fpga_plan.children = current_children
        }
      }

    }
  }

  def printBinaryOperation(b: BinaryNode,
                           num_indent: Int,
                           root_required_col: ListBuffer[String],
                           fpga_plan: SQL2FPGA_QPlan,
                           sf: Int): Unit = {
    print(num_indent.toString)
    print_indent(num_indent)
    var nodeType = ""
//    for (ptrn <- b.nodePatterns) {
//      print(ptrn.toString + " - ")
//      nodeType = nodeType.concat(ptrn.toString + "_")
//    }
    nodeType = b.nodeName
    fpga_plan.nodeType = nodeType
    print("\n")
    b match {
      case al_join: Join =>
        var nodeType = "JOIN_" + al_join.joinType.toString.toUpperCase()
        fpga_plan.nodeType = nodeType
        print_indent(num_indent)
        print("Output: ")
        for (_output <- root_required_col) {
          print(_output + ", ")
        }
        fpga_plan.outputCols = root_required_col
        print("\n")
        print_indent(num_indent)
        print("Process: ")
        var condition = new ListBuffer[String]()
        var joining_expr = new ListBuffer[Expression]()
        for (_condition <- al_join.condition) {
          println(_condition.getClass.getName)
          print(_condition.toString + ", ")
          condition += _condition.toString
          joining_expr += _condition
        }
        fpga_plan.operation = condition
        fpga_plan.joining_expression = joining_expr
        print("\n")
        print_indent(num_indent)
        print("Input left: ")
        println(al_join.left.output)
        print_indent(num_indent)
        print("Input right: ")
        println(al_join.right.output)

        var join_clauses = fpga_plan.getJoinKeyTerms(fpga_plan.joining_expression(0), false)
        var isSpecialSemiJoin = fpga_plan.nodeType == "JOIN_LEFTSEMI" && join_clauses.length == 2 &&
          ((join_clauses(0).contains("!=") && !join_clauses(1).contains("!=")) || (!join_clauses(0).contains("!=") && join_clauses(1).contains("!=")))
        fpga_plan.isSpecialSemiJoin = isSpecialSemiJoin
        var isSpecialAntiJoin = fpga_plan.nodeType == "JOIN_LEFTANTI" && join_clauses.length == 2 &&
          ((join_clauses(0).contains("!=") && !join_clauses(1).contains("!=")) || (!join_clauses(0).contains("!=") && join_clauses(1).contains("!=")))
        fpga_plan.isSpecialAntiJoin = isSpecialAntiJoin

      case al_other => println(al_other.getClass.getName)
    }
    for (ch <- b.children) {
      var next_fpga_plan = new SQL2FPGA_QPlan
      next_fpga_plan.treeDepth = fpga_plan.treeDepth + 1
      parse_optimized_query(ch, num_indent + 1, null, next_fpga_plan, sf)
      var current_children = fpga_plan.children
      current_children += next_fpga_plan
      fpga_plan.children = current_children
    }
  }

  def parse_optimized_query(plan: QueryPlan[_],
                            num_indent: Int,
                            root_required_col: ListBuffer[String],
                            fpga_plan: SQL2FPGA_QPlan,
                            scale_factor: Int): Unit = {
    var parent_required_col = new ListBuffer[String]()
    if (root_required_col == null || root_required_col.isEmpty) {
      for (_attr <- plan.outputSet) {
        parent_required_col += _attr.toString
      }
    } else {
      //parent_required_col = root_required_col
      for (root_col <- root_required_col) {
        if (!parent_required_col.contains(root_col)) {
          parent_required_col += root_col
        }
      }
    }
    print_indent(num_indent)
    println(parent_required_col.toString())
    plan match {
      case u: UnaryNode => printUnaryOperation(u, num_indent, parent_required_col, fpga_plan, scale_factor)
      case b: BinaryNode => printBinaryOperation(b, num_indent, parent_required_col, fpga_plan, scale_factor)
      case _ => Nil
    }
  }
  def parse_SparkQPlan_to_SQL2FPGAQPlan_TPCH(SparkOptQPlan: QueryPlan[_], qConfig: SQL2FPGA_QConfig, schemaProvider: TpchSchemaProvider): Unit = {
    var num_indent = 0
    // Init parsing
    parse_optimized_query(SparkOptQPlan, num_indent, null, _qPlan, qConfig.scale_factor)
    parse_optimized_query(SparkOptQPlan, num_indent, null, _qPlan_backup, qConfig.scale_factor)
    // Establish parent-children linking
    _qPlan.addChildrenParentConnections(schemaProvider.dfMap, qConfig.pure_sw_mode)
    _qPlan_backup.addChildrenParentConnections(schemaProvider.dfMap, qConfig.pure_sw_mode)
    // Debug printouts
    if (DEBUG_PARSER) {
      _qPlan.printPlan_InOrder(schemaProvider.dfMap)
      println(columnDictionary.toString())
    }
  }
  def parse_SparkQPlan_to_SQL2FPGAQPlan_TPCDS(SparkOptQPlan: QueryPlan[_], qConfig: SQL2FPGA_QConfig, schemaProvider: TpcdsSchemaProvider): Unit = {
    var num_indent = 0
    // Init parsing
    parse_optimized_query(SparkOptQPlan, num_indent, null, _qPlan, qConfig.scale_factor)
    parse_optimized_query(SparkOptQPlan, num_indent, null, _qPlan_backup, qConfig.scale_factor)
    // Establish parent-children linking
    _qPlan.addChildrenParentConnections(schemaProvider.dfMap, qConfig.pure_sw_mode)
    _qPlan_backup.addChildrenParentConnections(schemaProvider.dfMap, qConfig.pure_sw_mode)
    // Debug printouts
    if (DEBUG_PARSER) {
      _qPlan.printPlan_InOrder(schemaProvider.dfMap)
      println(columnDictionary.toString())
    }
  }
}

