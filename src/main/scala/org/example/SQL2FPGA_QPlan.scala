package org.example
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Expression
import org.example.SQL2FPGA_Top.{DEBUG_CASCADE_JOIN_OPT, DEBUG_REDUNDANT_OP_OPT, DEBUG_SPECIAL_JOIN_OPT, DEBUG_STRINGDATATYPE_OPT, columnDictionary}

import scala.collection.mutable.{ListBuffer, Queue}
import scala.math.{min, pow}

//----------------------------------------------------------------------------------------------------------------
// SQL2FPGA compiler backend - core code
//----------------------------------------------------------------------------------------------------------------
class SQL2FPGA_QPlan {
  private var _treeDepth: Int = 0
  private var _genCodeVisited: Boolean = false
  private var _genHostCodeVisited: Boolean = false
  private var _genFPGAConfigCodeVisited: Boolean = false
  private var _genSWConfigCodeVisited: Boolean = false
  private var _nodeType: String = "NULL"
  private var _inputCols: ListBuffer[String] = new ListBuffer[String]()
  private var _outputCols: ListBuffer[String] = new ListBuffer[String]()
  private var _outputCols_alias: ListBuffer[String] = new ListBuffer[String]()
  private var _numTableRow: Int = -1
  private var _operation: ListBuffer[String] = new ListBuffer[String]()
  private var _aggregate_operation: ListBuffer[String] = new ListBuffer[String]()
  private var _aggregate_expression: ListBuffer[Expression] = new ListBuffer[Expression]()
  private var _groupBy_operation: ListBuffer[String] = new ListBuffer[String]()
  private var _groupBy_expression: ListBuffer[Expression] = new ListBuffer[Expression]()
  private var _sorting_expression: ListBuffer[Expression] = new ListBuffer[Expression]()
  private var _joining_expression: ListBuffer[Expression] = new ListBuffer[Expression]()
  private var _isSpecialSemiJoin: Boolean = false
  private var _isSpecialAntiJoin: Boolean = false
  private var _stringRowIDSubstitution: Boolean = false
  private var _stringRowIDBackSubstitution: Boolean = false
  private var _filtering_expression: Expression = null
  private var _parent: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
  private var _children: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
  private var _bindedOverlayInstances: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
  private var _bindedOverlayInstances_left: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
  private var _bindedOverlayInstances_right: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
  private var _fpgaCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaNodeName: String = "NULL"
  private var _fpgaInputTableName: String = "NULL"
  private var _fpgaInputTableName_stringRowIDSubstitute: String = "NULL"
  private var _fpgaOutputTableName: String = "NULL"
  private var _fpgaOutputTableName_stringRowIDSubstitute: String = "NULL"
  private var _fpgaTransEngineName: String = "NULL"
  private var _fpgaEventsH2DName: String = "NULL"
  private var _fpgaEventsD2HName: String = "NULL"
  private var _fpgaEventsName: String = "NULL"
  private var _fpgaSWFuncName: String = "NULL"
  private var _cpuORfpga: Int = 1
  private var _fpgaOverlayID: Int = -1
  private var _fpgaOverlayType: Int = 0
  private var _fpgaJoinOverlayPipelineDepth: Int = 0
  private var _fpgaAggrOverlayPipelineDepth: Int = 0
  private var _fpgaJoinOverlayCallCount: Int = 0
  private var _fpgaAggrOverlayCallCount: Int = 0
  private var _fpgaInputCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaInputDevAllocateCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaConfigCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaConfigFuncCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaKernelSetupCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaTransEngineSetupCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaKernelEventsCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaKernelRunCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaSWCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaSWFuncCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaOutputCode: ListBuffer[String] = new ListBuffer[String]()
  private var _fpgaOutputDevAllocateCode: ListBuffer[String] = new ListBuffer[String]()
  private var _executionTimeCode: ListBuffer[String] = new ListBuffer[String]()

  def treeDepth = _treeDepth
  def genCodeVisited = _genCodeVisited
  def genHostCodeVisited = _genHostCodeVisited
  def genFPGAConfigCodeVisited = _genFPGAConfigCodeVisited
  def genSWConfigCodeVisited = _genSWConfigCodeVisited
  def nodeType = _nodeType
  def inputCols = _inputCols
  def outputCols = _outputCols
  def outputCols_alias = _outputCols_alias
  def numTableRow = _numTableRow
  def operation = _operation
  def aggregate_operation = _aggregate_operation
  def aggregate_expression = _aggregate_expression
  def groupBy_operation = _groupBy_operation
  def groupBy_expression = _groupBy_expression
  def sorting_expression = _sorting_expression
  def joining_expression = _joining_expression
  def isSpecialSemiJoin = _isSpecialSemiJoin
  def isSpecialAntiJoin = _isSpecialAntiJoin
  def stringRowIDSubstitution = _stringRowIDSubstitution
  def stringRowIDBackSubstitution = _stringRowIDBackSubstitution
  def filtering_expression = _filtering_expression
  def parent = _parent
  def children = _children
  def bindedOverlayInstances = _bindedOverlayInstances
  def bindedOverlayInstances_left = _bindedOverlayInstances_left
  def bindedOverlayInstances_right = _bindedOverlayInstances_right
  def fpgaCode = _fpgaCode
  def fpgaNodeName = _fpgaNodeName
  def fpgaInputTableName = _fpgaInputTableName
  def fpgaInputTableName_stringRowIDSubstitute = _fpgaInputTableName_stringRowIDSubstitute
  def fpgaOutputTableName = _fpgaOutputTableName
  def fpgaOutputTableName_stringRowIDSubstitute = _fpgaOutputTableName_stringRowIDSubstitute
  def fpgaTransEngineName = _fpgaTransEngineName
  def fpgaEventsH2DName = _fpgaEventsH2DName
  def fpgaEventsD2HName = _fpgaEventsD2HName
  def fpgaEventsName = _fpgaEventsName
  def fpgaSWFuncName = _fpgaSWFuncName
  def cpuORfpga = _cpuORfpga
  def fpgaOverlayID = _fpgaOverlayID
  def fpgaOverlayType = _fpgaOverlayType
  def fpgaJoinOverlayPipelineDepth = _fpgaJoinOverlayPipelineDepth
  def fpgaAggrOverlayPipelineDepth = _fpgaAggrOverlayPipelineDepth
  def fpgaJoinOverlayCallCount = _fpgaJoinOverlayCallCount
  def fpgaAggrOverlayCallCount = _fpgaAggrOverlayCallCount
  def fpgaInputCode = _fpgaInputCode
  def fpgaInputDevAllocateCode = _fpgaInputDevAllocateCode
  def fpgaConfigCode = _fpgaConfigCode
  def fpgaConfigFuncCode = _fpgaConfigFuncCode
  def fpgaKernelSetupCode = _fpgaKernelSetupCode
  def fpgaTransEngineSetupCode = _fpgaTransEngineSetupCode
  def fpgaKernelEventsCode = _fpgaKernelEventsCode
  def fpgaKernelRunCode = _fpgaKernelRunCode
  def fpgaSWCode = _fpgaSWCode
  def fpgaSWFuncCode = _fpgaSWFuncCode
  def fpgaOutputCode = _fpgaOutputCode
  def fpgaOutputDevAllocateCode = _fpgaOutputDevAllocateCode
  def executionTimeCode = _executionTimeCode

  def treeDepth_= (newValue: Int): Unit = {
    _treeDepth = newValue
  }
  def genCodeVisited_= (newValue: Boolean): Unit = {
    _genCodeVisited = newValue
  }
  def genHostCodeVisited_= (newValue: Boolean): Unit = {
    _genHostCodeVisited = newValue
  }
  def genFPGAConfigCodeVisited_= (newValue: Boolean): Unit = {
    _genFPGAConfigCodeVisited = newValue
  }
  def genSWConfigCodeVisited_= (newValue: Boolean): Unit = {
    _genSWConfigCodeVisited = newValue
  }
  def nodeType_= (newValue: String): Unit ={
    _nodeType = newValue
  }
  def inputCols_= (newValue: ListBuffer[String]): Unit ={
    _inputCols = newValue
  }
  def outputCols_= (newValue: ListBuffer[String]): Unit ={
    _outputCols = newValue
  }
  def outputCols_alias_= (newValue: ListBuffer[String]): Unit ={
    _outputCols_alias = newValue
  }
  def numTableRow_= (newValue: Int): Unit = {
    _numTableRow = newValue
  }
  def operation_= (newValue: ListBuffer[String]): Unit ={
    _operation = newValue
  }
  def groupBy_operation_= (newValue: ListBuffer[String]): Unit ={
    _groupBy_operation = newValue
  }
  def aggregate_operation_= (newValue: ListBuffer[String]): Unit ={
    _aggregate_operation = newValue
  }
  def aggregate_expression_= (newValue: ListBuffer[Expression]): Unit ={
    _aggregate_expression = newValue
  }
  def groupBy_expression_= (newValue: ListBuffer[Expression]): Unit ={
    _groupBy_expression = newValue
  }
  def sorting_expression_= (newValue: ListBuffer[Expression]): Unit ={
    _sorting_expression = newValue
  }
  def joining_expression_= (newValue: ListBuffer[Expression]): Unit ={
    _joining_expression = newValue
  }
  def isSpecialSemiJoin_= (newValue: Boolean): Unit ={
    _isSpecialSemiJoin = newValue
  }
  def isSpecialAntiJoin_= (newValue: Boolean): Unit ={
    _isSpecialAntiJoin = newValue
  }
  def stringRowIDSubstitution_= (newValue: Boolean): Unit ={
    _stringRowIDSubstitution = newValue
  }
  def stringRowIDBackSubstitution_= (newValue: Boolean): Unit ={
    _stringRowIDBackSubstitution = newValue
  }
  def filtering_expression_= (newValue: Expression): Unit ={
    _filtering_expression = newValue
  }
  def parent_= (newValue: ListBuffer[SQL2FPGA_QPlan]): Unit ={
    _parent = newValue
  }
  def children_= (newValue: ListBuffer[SQL2FPGA_QPlan]): Unit ={
    _children = newValue
  }
  def bindedOverlayInstances_= (newValue: ListBuffer[SQL2FPGA_QPlan]): Unit ={
    _bindedOverlayInstances = newValue
  }
  def bindedOverlayInstances_left_= (newValue: ListBuffer[SQL2FPGA_QPlan]): Unit ={
    _bindedOverlayInstances_left = newValue
  }
  def bindedOverlayInstances_right_= (newValue: ListBuffer[SQL2FPGA_QPlan]): Unit ={
    _bindedOverlayInstances_right = newValue
  }
  def fpgaCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaCode = newValue
  }
  def fpgaNodeName_= (newValue: String): Unit ={
    _fpgaNodeName = newValue
  }
  def fpgaInputTableName_= (newValue: String): Unit ={
    _fpgaInputTableName = newValue
  }
  def fpgaInputTableName_stringRowIDSubstitute_= (newValue: String): Unit ={
    _fpgaInputTableName_stringRowIDSubstitute = newValue
  }
  def fpgaOutputTableName_= (newValue: String): Unit ={
    _fpgaOutputTableName = newValue
  }
  def fpgaOutputTableName_stringRowIDSubstitute_= (newValue: String): Unit ={
    _fpgaOutputTableName_stringRowIDSubstitute = newValue
  }
  def fpgaTransEngineName_= (newValue: String): Unit ={
    _fpgaTransEngineName = newValue
  }
  def fpgaEventsH2DName_= (newValue: String): Unit ={
    _fpgaEventsH2DName = newValue
  }
  def fpgaEventsD2HName_= (newValue: String): Unit ={
    _fpgaEventsD2HName = newValue
  }
  def fpgaEventsName_= (newValue: String): Unit ={
    _fpgaEventsName = newValue
  }
  def fpgaSWFuncName_= (newValue: String): Unit ={
    _fpgaSWFuncName = newValue
  }
  def cpuORfpga_= (newValue: Int): Unit = {
    _cpuORfpga = newValue
  }
  def fpgaOverlayID_= (newValue: Int): Unit = {
    _fpgaOverlayID = newValue
  }
  def fpgaOverlayType_= (newValue: Int): Unit = {
    _fpgaOverlayType = newValue
  }
  def fpgaJoinOverlayPipelineDepth_= (newValue: Int): Unit = {
    _fpgaJoinOverlayPipelineDepth = newValue
  }
  def fpgaAggrOverlayPipelineDepth_= (newValue: Int): Unit = {
    _fpgaAggrOverlayPipelineDepth = newValue
  }
  def fpgaJoinOverlayCallCount_= (newValue: Int): Unit = {
    _fpgaJoinOverlayCallCount = newValue
  }
  def fpgaAggrOverlayCallCount_= (newValue: Int): Unit = {
    _fpgaAggrOverlayCallCount = newValue
  }
  def fpgaInputCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaInputCode = newValue
  }
  def fpgaInputDevAllocateCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaInputDevAllocateCode = newValue
  }
  def fpgaConfigCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaConfigCode = newValue
  }
  def fpgaConfigFuncCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaConfigFuncCode = newValue
  }
  def fpgaKernelSetupCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaKernelSetupCode = newValue
  }
  def fpgaTransEngineSetupCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaTransEngineSetupCode = newValue
  }
  def fpgaKernelEventsCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaKernelEventsCode = newValue
  }
  def fpgaKernelRunCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaKernelRunCode = newValue
  }
  def fpgaSWCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaSWCode = newValue
  }
  def fpgaSWFuncCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaSWFuncCode = newValue
  }
  def fpgaOutputCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaOutputCode = newValue
  }
  def fpgaOutputDevAllocateCode_= (newValue: ListBuffer[String]): Unit ={
    _fpgaOutputDevAllocateCode = newValue
  }
  def executionTimeCode_= (newValue: ListBuffer[String]): Unit ={
    _executionTimeCode = newValue
  }

  def getColumnDataType(df: DataFrame, col_name: String): String = {
    var result = ""
    for (col <- df.schema) {
      if (col.name == col_name) {
        result = col.dataType.toString
      }
    }
    result
  }

  def getColumnType(raw_col_name: String, dfmap: Map[String, DataFrame]): String = {
    var result = ""
    if (columnDictionary(raw_col_name)._2 == "NULL") {
      result = columnDictionary(raw_col_name)._1
    } else {
      if (columnDictionary(raw_col_name)._1 == "IntegerType" ||
        columnDictionary(raw_col_name)._1 == "LongType" ||
        columnDictionary(raw_col_name)._1 == "StringType" ||
        columnDictionary(raw_col_name)._1 == "DoubleType" ||
        columnDictionary(raw_col_name)._1 == "DecimalType") {
        result = columnDictionary(raw_col_name)._1
      } else {
        result = getColumnDataType(dfmap(columnDictionary(raw_col_name)._1), columnDictionary(raw_col_name)._2)
      }
    }
    result
  }

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

  def stripColumnName(raw_col_name: String): String = {
    var result = ""
    result = raw_col_name.replaceAll("#", "")
    // To cover the case like: (0.2 * avg(l_quantity))#367
    result = result.replaceAll("\\(", "")
    result = result.replaceAll("\\)", "")
    // To cover the case like: (sum((ps_supplycost * ps_availqty)) * 0.0001000000)#368
    result = result.replaceAll("\\.", "")

    result = result.split(" ").last
    result = "_" + result
    result
  }

  def print_indent(num_indent: Int): Unit = {
    for (it <- 0 until (num_indent)) {
      print("\t")
    }
  }

  def deepCopy(): SQL2FPGA_QPlan={
    var deepCopyNode = new SQL2FPGA_QPlan

    deepCopyNode._treeDepth = this._treeDepth
    deepCopyNode._nodeType = this._nodeType
    deepCopyNode._inputCols = this._inputCols.clone()
    deepCopyNode._outputCols = this._outputCols.clone()
    deepCopyNode._outputCols_alias = this._outputCols_alias.clone()
    deepCopyNode._numTableRow = this._numTableRow
    deepCopyNode._operation = this._operation.clone()
    deepCopyNode._aggregate_operation = this._aggregate_operation.clone()
    deepCopyNode._aggregate_expression = this._aggregate_expression.clone()
    deepCopyNode._groupBy_operation = this._groupBy_operation.clone()
    deepCopyNode._sorting_expression = this._sorting_expression.clone()
    deepCopyNode._joining_expression = this._joining_expression.clone()
    deepCopyNode._isSpecialSemiJoin = this._isSpecialSemiJoin
    deepCopyNode._isSpecialAntiJoin = this._isSpecialAntiJoin
    deepCopyNode._filtering_expression = this._filtering_expression
    var newChildren = new ListBuffer[SQL2FPGA_QPlan]()
    for (ch <- this._children) {
      newChildren += ch.deepCopy()
    }
    deepCopyNode._children = newChildren
    //      deepCopyNode._bindedOverlayInstances: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
    //      deepCopyNode._bindedOverlayInstances_left: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
    //      deepCopyNode._bindedOverlayInstances_right: ListBuffer[SQL2FPGA_QPlan] = new ListBuffer[SQL2FPGA_QPlan]()
    deepCopyNode._fpgaCode = this._fpgaCode.clone()
    deepCopyNode._fpgaNodeName = this._fpgaNodeName
    deepCopyNode._fpgaInputTableName = this._fpgaInputTableName
    deepCopyNode._fpgaOutputTableName = this._fpgaOutputTableName
    deepCopyNode._fpgaTransEngineName = this._fpgaTransEngineName
    deepCopyNode._fpgaEventsH2DName = this._fpgaEventsH2DName
    deepCopyNode._fpgaEventsD2HName = this._fpgaEventsD2HName
    deepCopyNode._fpgaEventsName = this._fpgaEventsName
    deepCopyNode._fpgaSWFuncName = this._fpgaSWFuncName
    deepCopyNode._cpuORfpga = this._cpuORfpga
    deepCopyNode._fpgaOverlayID = this._fpgaOverlayID
    deepCopyNode._fpgaOverlayType = this._fpgaOverlayType
    deepCopyNode._fpgaJoinOverlayPipelineDepth = this._fpgaJoinOverlayPipelineDepth
    deepCopyNode._fpgaAggrOverlayPipelineDepth = this._fpgaAggrOverlayPipelineDepth
    deepCopyNode._fpgaJoinOverlayCallCount = this._fpgaJoinOverlayCallCount
    deepCopyNode._fpgaAggrOverlayCallCount = this._fpgaAggrOverlayCallCount
    deepCopyNode._fpgaInputCode = this._fpgaInputCode.clone()
    deepCopyNode._fpgaInputDevAllocateCode = this._fpgaInputDevAllocateCode.clone()
    deepCopyNode._fpgaConfigCode = this._fpgaConfigCode.clone()
    deepCopyNode._fpgaConfigFuncCode = this._fpgaConfigFuncCode.clone()
    deepCopyNode._fpgaKernelSetupCode = this._fpgaKernelSetupCode.clone()
    deepCopyNode._fpgaTransEngineSetupCode = this._fpgaTransEngineSetupCode.clone()
    deepCopyNode._fpgaKernelEventsCode = this._fpgaKernelEventsCode.clone()
    deepCopyNode._fpgaKernelRunCode = this._fpgaKernelRunCode.clone()
    deepCopyNode._fpgaSWCode = this._fpgaSWCode.clone()
    deepCopyNode._fpgaSWFuncCode = this._fpgaSWFuncCode.clone()
    deepCopyNode._fpgaOutputCode = this._fpgaOutputCode.clone()
    deepCopyNode._fpgaOutputDevAllocateCode = this._fpgaOutputDevAllocateCode.clone()
    deepCopyNode._executionTimeCode = this._executionTimeCode.clone()

    return deepCopyNode
  }

  def getLeftMostBindedOperator(thisNode: SQL2FPGA_QPlan): SQL2FPGA_QPlan={
    if (thisNode._bindedOverlayInstances.isEmpty && thisNode._bindedOverlayInstances_left.isEmpty) {
      return thisNode
    }

    if (thisNode._bindedOverlayInstances_left.nonEmpty) {
      return getLeftMostBindedOperator(thisNode._bindedOverlayInstances_left.head)
    }
    else if (thisNode._bindedOverlayInstances.nonEmpty) {
      return getLeftMostBindedOperator(thisNode._bindedOverlayInstances.head)
    }
    else { //Error Case
      return null
    }
  }

  def getRightMostBindedOperator(thisNode: SQL2FPGA_QPlan): SQL2FPGA_QPlan={
    if (thisNode._bindedOverlayInstances.isEmpty && thisNode._bindedOverlayInstances_right.isEmpty) {
      return thisNode
    }

    if (thisNode._bindedOverlayInstances_right.nonEmpty) {
      return getRightMostBindedOperator(thisNode._bindedOverlayInstances_right.head)
    }
    else if (thisNode._bindedOverlayInstances.nonEmpty) {
      return getRightMostBindedOperator(thisNode._bindedOverlayInstances.head)
    }
    else { //Error Case
      return null
    }
  }

  def getInnerMostBindedOperator(thisNode: SQL2FPGA_QPlan): SQL2FPGA_QPlan={
    if (thisNode._bindedOverlayInstances.isEmpty) {
      return thisNode
    }

    if (thisNode._bindedOverlayInstances.nonEmpty) {
      return getInnerMostBindedOperator(thisNode._bindedOverlayInstances.head)
    }
    else { //Error Case
      return null
    }
  }

  def getJoinOperator(thisNode: SQL2FPGA_QPlan): SQL2FPGA_QPlan={
    if (thisNode._nodeType == "JOIN_INNER" || thisNode._nodeType == "JOIN_LEFTANTI" || thisNode._nodeType == "JOIN_LEFTSEMI") {
      return thisNode
    }

    if (thisNode._bindedOverlayInstances.nonEmpty) {
      return getJoinOperator(thisNode._bindedOverlayInstances.head)
    }
    else { //Error Case
      return null
    }
  }

  def getNonGroupByAggregateOperator(thisNode: SQL2FPGA_QPlan): SQL2FPGA_QPlan={
    // if (thisNode._nodeType == "Aggregate" && thisNode._groupBy_operation.isEmpty) {
    if ((thisNode._nodeType == "Aggregate" && thisNode._groupBy_operation.isEmpty) || thisNode._nodeType == "Project") {
      return thisNode
    }

    if (thisNode._bindedOverlayInstances.nonEmpty) {
      return getNonGroupByAggregateOperator(thisNode._bindedOverlayInstances.head)
    }
    else { //Error Case
      return null
    }
  }

  def getFilterOperator(thisNode: SQL2FPGA_QPlan): SQL2FPGA_QPlan={
    if (thisNode._nodeType == "Filter") {
      return thisNode
    }

    if (thisNode._bindedOverlayInstances.nonEmpty) {
      return getFilterOperator(thisNode._bindedOverlayInstances.head)
    }
    else { //Error Case
      return null
    }
  }

  def getGroupByAggregateOperator(thisNode: SQL2FPGA_QPlan): SQL2FPGA_QPlan={
    if (thisNode._nodeType == "Aggregate" && thisNode._groupBy_operation.nonEmpty) {
      return thisNode
    }

    if (thisNode._bindedOverlayInstances.nonEmpty) {
      return getGroupByAggregateOperator(thisNode._bindedOverlayInstances.head)
    }
    else { //Error Case
      return null
    }
  }

  def getFilterConfigFuncCode(filterConfigFuncName: String, filterNode: SQL2FPGA_QPlan, col_idx_dict: collection.mutable.Map[String, Int]): ListBuffer[String] = {
    var filterCfgFuncCode = new ListBuffer[String]()
    filterCfgFuncCode += "static void " + filterConfigFuncName + "(uint32_t cfg[]) {"
    filterCfgFuncCode += "    using namespace xf::database;"
    filterCfgFuncCode += "    int n = 0;"

    // filterConditions_const => Map(col_idx, [filter_val, filter_op])
    val filterConditions_const = collection.mutable.Map[Int, ListBuffer[(String, String)]]()
    val filterConditions_col = collection.mutable.Map[(Int, Int), String]()
    //      var filter_clauses = filterNode._operation.head.stripPrefix("(").stripSuffix(")").split(" AND ")
    var filter_clauses = getFilterClause(filterNode._filtering_expression)
    for (clause <- filter_clauses) {
      if (!clause.contains("isnotnull")) {
        var clause_formatted = clause
        //          while (clause_formatted.contains("(") | clause_formatted.contains(")")) {
        //            clause_formatted = clause_formatted.replace("(", "")
        //            clause_formatted = clause_formatted.replace(")", "")
        //          }
        // fix: filtering col name
        clause_formatted = clause_formatted.stripPrefix("(")
        clause_formatted = clause_formatted.stripSuffix(")")
        var col_name = clause_formatted.split(" ").head
        var col_val = clause_formatted.split(" ").last
        var filter_clause_col_idx = col_idx_dict(col_name)
        var filter_clause_val_idx = -1
        if (col_idx_dict.contains(col_val))
          filter_clause_val_idx = col_idx_dict(col_val)

        var col_op = "FOP_DC"
        if (clause_formatted.contains(" = ")) {
          col_op = "FOP_EQ"
        }
        else if (clause_formatted.contains(" != ")) {
          col_op = "FOP_NE"
        }
        else if (clause_formatted.contains(" > ")) {
          col_op = "FOP_GTU"
        }
        else if (clause_formatted.contains(" < ")) {
          col_op = "FOP_LTU"
        }
        else if (clause_formatted.contains(" >= ")) {
          col_op = "FOP_GEU"
        }
        else if (clause_formatted.contains(" <= ")) {
          col_op = "FOP_LEU"
        }
        else {
          col_op = "FOP_DC"
        }

        if (filter_clause_val_idx == -1) { //const filtering factor
          // e.g. columnDictionary += (col -> (tcph_table, col_first))
          if (filterConditions_const.contains(filter_clause_col_idx)) {
            var filterValOp = filterConditions_const(filter_clause_col_idx)
            var temp = (col_val, col_op)
            filterValOp += temp
            filterConditions_const += (filter_clause_col_idx -> filterValOp)
          }
          else {
            var filterValOp = new ListBuffer[(String, String)]()
            var temp = (col_val, col_op)
            filterValOp += temp
            filterConditions_const += (filter_clause_col_idx -> filterValOp)
          }
        } else {
          if (filter_clause_col_idx > filter_clause_val_idx) {
            val temp = (filter_clause_val_idx, filter_clause_col_idx)
            if (col_op == "FOP_GTU") {
              col_op = "FOP_LTU"
            }
            else if (col_op == "FOP_LTU") {
              col_op = "FOP_GTU"
            }
            else if (col_op == "FOP_GEU") {
              col_op = "FOP_LEU"
            }
            else if (col_op == "FOP_LEU") {
              col_op = "FOP_GEU"
            }
            filterConditions_col += (temp -> col_op)
          } else {
            val temp = (filter_clause_col_idx, filter_clause_val_idx)
            filterConditions_col += (temp -> col_op)
          }
        }
      }
    }
    println(filterConditions_const)
    // check if map col is <= 4 and the number of filter condition is <= 2
    var num_col = filterConditions_const.size
    if (num_col > 4) {
      filterCfgFuncCode += "//Unsupported number of filter columns (num_col > 4)"
    }
    for ((key, payload) <- filterConditions_const) {
      if (payload.length > 2) {
        filterCfgFuncCode += "//Unsupported number of filter conditions (num_col > 2)"
      }
    }

    var i = 0
    for (i <- 0 to 4 - 1) {
      filterCfgFuncCode += "    // cond_" + (i + 1).toString
      if (filterConditions_const.contains(i)) {
        var payloadValOp = filterConditions_const(i)
        if (payloadValOp.length == 1) {
          if (payloadValOp.head._2 == "FOP_LTU" || payloadValOp.head._2 == "FOP_LEU"
            || payloadValOp.head._2 == "FOP_EQ" || payloadValOp.head._2 == "FOP_NE") {
            filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
            filterCfgFuncCode += "    cfg[n++] = (uint32_t)" + payloadValOp.head._1 + "UL;"
            filterCfgFuncCode += "    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (" + payloadValOp.head._2 + ");"
          }
          else if (payloadValOp.head._2 == "FOP_GTU" || payloadValOp.head._2 == "FOP_GEU"
            || payloadValOp.head._2 == "FOP_EQ" || payloadValOp.head._2 == "FOP_NE") {
            filterCfgFuncCode += "    cfg[n++] = (uint32_t)" + payloadValOp.head._1 + "UL;"
            filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
            filterCfgFuncCode += "    cfg[n++] = 0UL | (" + payloadValOp.head._2 + " << FilterOpWidth) | (FOP_DC);"
          }
          else {
            filterCfgFuncCode += "    //Unsupported Op"
          }
        }
        else if (payloadValOp.length == 2) {
          filterCfgFuncCode += "    cfg[n++] = (uint32_t)" + payloadValOp.head._1 + "UL;"
          filterCfgFuncCode += "    cfg[n++] = (uint32_t)" + payloadValOp.last._1 + "UL;"
          filterCfgFuncCode += "    cfg[n++] = 0UL | (" + payloadValOp.head._2 + " << FilterOpWidth) | (" + payloadValOp.last._2 + ");"
        }
        else {
          filterCfgFuncCode += "    //Unsupported Op"
        }
      }
      else {
        filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
        filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
        filterCfgFuncCode += "    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (FOP_DC);"
      }
    }
    filterCfgFuncCode += "    "
    filterCfgFuncCode += "    uint32_t r = 0;"
    filterCfgFuncCode += "    int sh = 0;"
    var j = 0
    i = 0
    for (i <- 0 to 2) {
      for (j <- i + 1 to 3) {
        if (filterConditions_col.contains((i, j))) {
          filterCfgFuncCode += "    r |= ((uint32_t)(" + filterConditions_col(i, j) + " << sh));"
          filterCfgFuncCode += "    sh += FilterOpWidth;"
        } else {
          filterCfgFuncCode += "    r |= ((uint32_t)(FOP_DC << sh));"
          filterCfgFuncCode += "    sh += FilterOpWidth;"
        }
      }
    }
    filterCfgFuncCode += "    cfg[n++] = r;" + "\n"
    filterCfgFuncCode += "    // 4 true and 6 true"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
    filterCfgFuncCode += "    cfg[n++] = (uint32_t)(1UL << 31);"
    filterCfgFuncCode += "}"

    return filterCfgFuncCode
  }

  def getStringLengthMacro(tbl_col: (String, String)): String = {
    // TPCH_READ_DATE_LEN - is not used anywhere, yet
    if (tbl_col._1 == "lineitem") {
      if        (tbl_col._2 == "l_returnflag")   { return "RANDOM_LEN"
      } else if (tbl_col._2 == "l_linestatus")   { return "RANDOM_LEN"
      } else if (tbl_col._2 == "l_shipinstruct") { return "TPCH_READ_MAXAGG_LEN"
      } else if (tbl_col._2 == "l_shipmode")     { return "TPCH_READ_MAXAGG_LEN"
      } else if (tbl_col._2 == "l_comment")      { return "TPCH_READ_L_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    } else if (tbl_col._1 == "supplier") {
      if        (tbl_col._2 == "s_name")    { return "TPCH_READ_S_NAME_LEN"
      } else if (tbl_col._2 == "s_address") { return "TPCH_READ_S_ADDR_MAX"
      } else if (tbl_col._2 == "s_phone")   { return "TPCH_READ_PHONE_LEN"
      } else if (tbl_col._2 == "s_comment") { return "TPCH_READ_S_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    } else if (tbl_col._1 == "nation") {
      if        (tbl_col._2 == "n_name")    { return "TPCH_READ_NATION_LEN"
      } else if (tbl_col._2 == "n_comment") { return "TPCH_READ_N_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    } else if (tbl_col._1 == "order") {
      if        (tbl_col._2 == "o_orderstatus")   { return "1" // single character
      } else if (tbl_col._2 == "o_orderpriority") { return "TPCH_READ_MAXAGG_LEN"
      } else if (tbl_col._2 == "o_clerk")         { return "TPCH_READ_O_CLRK_LEN"
      } else if (tbl_col._2 == "o_comment")       { return "TPCH_READ_O_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    } else if (tbl_col._1 == "customer") {
      if        (tbl_col._2 == "c_name")       { return "TPCH_READ_C_NAME_LEN"
      } else if (tbl_col._2 == "c_address")    { return "TPCH_READ_C_ADDR_MAX"
      } else if (tbl_col._2 == "c_phone")      { return "TPCH_READ_PHONE_LEN"
      } else if (tbl_col._2 == "c_mktsegment") { return "TPCH_READ_MAXAGG_LEN"
      } else if (tbl_col._2 == "c_comment")    { return "TPCH_READ_C_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    } else if (tbl_col._1 == "region") {
      if        (tbl_col._2 == "r_name")    { return "TPCH_READ_REGION_LEN"
      } else if (tbl_col._2 == "r_comment") { return "TPCH_READ_R_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    } else if (tbl_col._1 == "part") {
      if        (tbl_col._2 == "p_name")      { return "TPCH_READ_P_NAME_LEN"
      } else if (tbl_col._2 == "p_mfgr")      { return "TPCH_READ_P_MFG_LEN"
      } else if (tbl_col._2 == "p_brand")     { return "TPCH_READ_P_BRND_LEN"
      } else if (tbl_col._2 == "p_type")      { return "TPCH_READ_P_TYPE_LEN"
      } else if (tbl_col._2 == "p_container") { return "TPCH_READ_P_CNTR_LEN"
      } else if (tbl_col._2 == "p_comment")   { return "TPCH_READ_P_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    } else if (tbl_col._1 == "partsupp") {
      if (tbl_col._2 == "ps_comment") { return "TPCH_READ_PS_CMNT_MAX"
      } else { return "TPCH_READ_REGION_LEN"
      }
    }
    // Start of TPCDS dataset
    //      else if (tbl_col._1 == "item") {
    //        if (tbl_col._2 == "i_brand") { return "TPCDS_READ_MAX" //"TPCDS_READ_I_BRAND_MAX"
    //        } else { return tbl_col._2
    //        }
    //      }
    else {
      // return "TPCH_READ_REGION_LEN"
      return "TPCDS_READ_MAX"
      // return tbl_col._2
    }
  }

  def getFilterExpression(expr: Expression, children: ListBuffer[SQL2FPGA_QPlan] = ListBuffer.empty[SQL2FPGA_QPlan]): (ListBuffer[String], String) = {
    var prereq_str = new ListBuffer[String]

    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return (prereq_str, stripColumnName(expr.toString))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      if (expr.dataType.toString == "StringType"){
        //return ( "\"" + stripColumnName(expr.toString) + "\"")
        return (prereq_str, ( "\"" + expr.toString + "\""))
      }
      else {
        //stripColumnName(expr.toString)
        return (prereq_str, expr.toString)
      }
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.IsNotNull") {
      if (expr.dataType.toString == "StringType"){
        return (prereq_str, "（std::string(" + stripColumnName(expr.toString) + ".data()) != \"\")")
      }
      else {
        return (prereq_str, "(1)")
      }
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + left_sub._2 + " && " + right_sub._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + left_sub._2 + " || " + right_sub._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        var this_expr = getFilterExpression(expr.children(0), children)
        prereq_str ++= this_expr._1
        return (prereq_str, "!" + "(" + this_expr._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Like") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        // Line below assumes the type is stringType
        //std::regex_match(o_comment.data(), std::regex("(.*)(special)(.*)(requests)(.*)"))
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        var temp_str = right_sub._2.stripSuffix("\"").stripPrefix("\"").stripPrefix("%").split("%")
        if (temp_str.length == 2) {
          prereq_str += "struct custom_func {"
          prereq_str += "        bool strm_pattern(std::string sub1, std::string sub2, std::string s, int len = 7) {"
          prereq_str += "            std::string::size_type spe_f = s.find(sub1);"
          prereq_str += "            std::string::size_type c_f = 0;"
          prereq_str += "            while (spe_f != std::string::npos) {"
          prereq_str += "                c_f += (spe_f + len);"
          prereq_str += "                std::string sub_s = s.substr(c_f);"
          prereq_str += "                if (sub_s.find(sub2) != std::string::npos) return true;"
          prereq_str += "                spe_f = sub_s.find(sub1);"
          prereq_str += "            }"
          prereq_str += "            return false;"
          prereq_str += "        }"
          prereq_str += "}custom_func_obj;"
          return (prereq_str, "custom_func_obj.strm_pattern(" + "\"" + temp_str(0) + "\"" + ", " + "\"" +temp_str(1) + "\"" + ", " + left_sub._2 + ".data()" + ")")
        }
        else {
          return (prereq_str, "std::regex_match(" + left_sub._2 + ".data()" + ", " + "std::regex(" + right_sub._2.replaceAll("%", "(.*)") + ")" + ")")
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        if (expr.children(0).dataType.toString == "StringType") {
          return (prereq_str, "(" + "std::string(" + left_sub._2 + ".data())" + " == " + "\"" + expr.children(1).toString + "\"" + ")")
        }
        else if (expr.children(1).dataType.toString == "StringType") {
          return (prereq_str, "(" + "\"" + expr.children(0).toString + "\"" + " == " + "std::string(" + right_sub._2 + ".data())" + ")")
        }
        else {
          return (prereq_str, "(" + left_sub._2 + " == " + right_sub._2 + ")")
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + left_sub._2 + " >= " + right_sub._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + left_sub._2 + " <= " + right_sub._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThan") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + left_sub._2 + " > " + right_sub._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + left_sub._2 + " < " + right_sub._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.In") {
        var expr_type = expr.children(0).dataType.toString
        var target_col_str = getFilterExpression(expr.children(0), children)
        prereq_str ++= target_col_str._1
        var set_col = expr.children.drop(1) //dropping the first item, which is the target_col
        var filtering_term = "("
        var first_col = true
        var reuse_col_str = ""
        var reuse_col_str_name = "reuse_col_str_" + scala.util.Random.nextInt(1000).toString
        for (this_col <- set_col) {
          var this_col_str = getFilterExpression(this_col, children)
          prereq_str ++= this_col_str._1
          if (expr_type == "StringType") {
            if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
              reuse_col_str = "auto " + reuse_col_str_name + " = " + "std::string(" + target_col_str._2 + ".data());"
              filtering_term += "(" + reuse_col_str_name + " == " + this_col_str._2 + ")" + " || "
            } else {
              reuse_col_str = "auto " + reuse_col_str_name + " = " + target_col_str._2.stripPrefix("(") + ";"
              filtering_term += "(" + reuse_col_str_name + " == " + this_col_str._2 + ")" + " || "
            }
          }
          else if (expr_type == "IntegerType" || expr_type == "LongType" || expr_type == "DoubleType"){
            // filtering_term += "Unsupported filtering type"
            reuse_col_str = "auto " + reuse_col_str_name + " = " + target_col_str._2 + ";"
            filtering_term += "(" + reuse_col_str_name + " == " + this_col + ")" + " || "
          }
          else{
            filtering_term += "Unrecognized filtering type"
          }

          if (first_col) {
            prereq_str += reuse_col_str
            first_col = false
          }
        }
        filtering_term += "(0)" //to complete the additional "||" term
        filtering_term += ")"
        return (prereq_str, filtering_term)
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        var phrase_length = right_sub._2.length - 2 // -2 is to get rid of the length from the quotation characters "xxx"
        return (prereq_str, "(" + "std::string(" + left_sub._2 + ".data()).find(" + right_sub._2 + ")" + "=="  + "std::string(" + left_sub._2 + ".data()).length() - " + phrase_length + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + "std::string(" + left_sub._2 + ".data()).find(" + right_sub._2 + ")" + "=="  + "0" + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
        var left_sub = getFilterExpression(expr.children(0), children)
        var right_sub = getFilterExpression(expr.children(1), children)
        prereq_str ++= left_sub._1
        prereq_str ++= right_sub._1
        return (prereq_str, "(" + "std::string(" + left_sub._2 + ".data()).find(" + right_sub._2 + ")" + "!="  + "std::string::npos" + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring") {
        var target_str = getFilterExpression(expr.children(0), children)
        var target_start_pos = (getFilterExpression(expr.children(1), children)._2.toInt - 1).toString
        var target_str_len = getFilterExpression(expr.children(2), children)
        prereq_str ++= target_str._1
        prereq_str ++= target_str_len._1
        return  (prereq_str, "(" + "std::string(" + target_str._2 + ".data())" + ".substr(" + target_start_pos + ", " + target_str_len._2 + ")")
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.ScalarSubquery") {
        //Alec-added: the line below assumes there is only one subquery (i.e., children(1)),
        //            and it only has one output (i.e., outputCols(0))
        var col_symbol = children(1).operation(0).split(" AS ").last
        var col_symbol_trimmed = stripColumnName(col_symbol)
        return (prereq_str, columnDictionary(col_symbol_trimmed)._1)
      }
      else {
        println(expr.getClass.getName)
        return getFilterExpression(expr.children(0), children)
      }
    }
  }

  def getJoinExpression(expr: Expression): String = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return expr.toString
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      if (expr.dataType.toString == "StringType"){
        return ( "\"" + expr.toString + "\"")
      }
      else {
        return expr.toString
      }
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getJoinExpression(expr.children(0))
        var right_sub = getJoinExpression(expr.children(1))
        return "(" + left_sub + " && " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getJoinExpression(expr.children(0))
        var right_sub = getJoinExpression(expr.children(1))
        return "(" + left_sub + " || " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        var left_sub = getJoinExpression(expr.children(0))
        var right_sub = getJoinExpression(expr.children(1))
        return "(" + left_sub + " == " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual") {
        var left_sub = getJoinExpression(expr.children(0))
        var right_sub = getJoinExpression(expr.children(1))
        return "(" + left_sub + " >= " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual") {
        var left_sub = getJoinExpression(expr.children(0))
        var right_sub = getJoinExpression(expr.children(1))
        return "(" + left_sub + " <= " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThan") {
        var left_sub = getJoinExpression(expr.children(0))
        var right_sub = getJoinExpression(expr.children(1))
        return "(" + left_sub + " > " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan") {
        var left_sub = getJoinExpression(expr.children(0))
        var right_sub = getJoinExpression(expr.children(1))
        return "(" + left_sub + " < " + right_sub + ")"
      }
      else {
        return getJoinExpression(expr.children(0))
      }
    }
  }

  def getJoinKeyExpression(expr: Expression): String = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return expr.toString
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getJoinKeyExpression(expr.children(0))
        var right_sub = getJoinKeyExpression(expr.children(1))
        if (left_sub != "NULL" && right_sub != "NULL") {
          return "(" + left_sub + " && " + right_sub + ")"
        }
        else if (left_sub != "NULL") {
          return left_sub
        }
        else if (right_sub != "NULL") {
          return right_sub
        }
        else {
          return "NULL"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getJoinKeyExpression(expr.children(0))
        var right_sub = getJoinKeyExpression(expr.children(1))
        if (left_sub != "NULL" && right_sub != "NULL") {
          return "(" + left_sub + " || " + right_sub + ")"
        }
        else if (left_sub != "NULL") {
          return left_sub
        }
        else if (right_sub != "NULL") {
          return right_sub
        }
        else {
          return "NULL"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference"
          && expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference")
        {
          var left_sub = getJoinKeyExpression(expr.children(0))
          var right_sub = getJoinKeyExpression(expr.children(1))
          return "(" + left_sub + " == " + right_sub + ")"
        }
        else {
          return "NULL"
        }
      }
      else {
        return "NULL"
      }
    }
  }

  def getJoinFilterExpression(expr: Expression): String = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return stripColumnName(expr.toString)
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      if (expr.dataType.toString == "StringType"){
        // return ( "\"" + stripColumnName(expr.toString) + "\"")
        return ( "\"" + expr.toString + "\"")
      }
      else {
        // return stripColumnName(expr.toString)
        return expr.toString
      }
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        if (left_sub != "NULL" && right_sub != "NULL") {
          return "(" + left_sub + " && " + right_sub + ")"
        }
        else if (left_sub != "NULL") {
          return left_sub
        }
        else if (right_sub != "NULL") {
          return right_sub
        }
        else {
          return "NULL"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        if (left_sub != "NULL" && right_sub != "NULL") {
          return "(" + left_sub + " || " + right_sub + ")"
        }
        else if (left_sub != "NULL") {
          return left_sub
        }
        else if (right_sub != "NULL") {
          return right_sub
        }
        else {
          return "NULL"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        if (expr.children(0).getClass.getName != "org.apache.spark.sql.catalyst.expressions.AttributeReference"
          || expr.children(1).getClass.getName != "org.apache.spark.sql.catalyst.expressions.AttributeReference")
        {
          var left_sub = getJoinFilterExpression(expr.children(0))
          var right_sub = getJoinFilterExpression(expr.children(1))
          return "(" + left_sub + " == " + right_sub + ")"
        }
        else {
          return "NULL"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        return "(" + left_sub + " >= " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        return "(" + left_sub + " <= " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThan") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        return "(" + left_sub + " > " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        return "(" + left_sub + " < " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.In") {
        var expr_type = expr.children(0).dataType.toString
        var target_col_str = getJoinFilterExpression(expr.children(0))
        var set_col = expr.children.drop(1) //dropping the first item, which is the target_col
        var filtering_term = "("
        for (this_col <- set_col){
          var this_col_str = getJoinFilterExpression(this_col)
          if (expr_type == "StringType") {
            filtering_term += "(" + target_col_str + " == " + this_col_str + ")" + " || "
          }
          else if (expr_type == "IntegerType" || expr_type == "LongType" || expr_type == "DoubleType"){
            // filtering_term += "Unsupported filtering type"
            filtering_term += "(" + target_col_str + " == " + this_col_str + ")" + " || "
          }
          else{
            filtering_term += "Unrecognized filtering type: " + expr_type
          }
        }
        filtering_term += "(0)" //to complete the additional "||" term
        filtering_term += ")"
        return filtering_term
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        var phrase_length = right_sub.length - 2 // -2 is to get rid of the length from the quotation characters "xxx"
        return "(" + left_sub + ".find(" + right_sub + ")" + "==" + left_sub + ".length() - " + phrase_length + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        return "(" + left_sub + ".find(" + right_sub + ")" + "=="  + "0" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
        var left_sub = getJoinFilterExpression(expr.children(0))
        var right_sub = getJoinFilterExpression(expr.children(1))
        return "(" + left_sub + ".find(" + right_sub + ")" + "!="  + "std::string::npos" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Cast") {
        return getJoinFilterExpression(expr.children(0))
      }
      else {
        println(expr.getClass.getName)
        return "NULL"
      }
    }
  }

  def getJoinTerms(expr: Expression): ListBuffer[String] = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return null
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      return null
    }
    else {
      var terms = new ListBuffer[String]()
      for (ch <- expr.children) {
        var children_term = getJoinTerms(ch)
        if (children_term != null)
          terms ++= children_term
      }
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        terms += expr.toString
      }
      return terms
    }
  }

  def getJoinKeyTerms(expr: Expression, negate: Boolean): ListBuffer[String] = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      var key_term = new ListBuffer[String]()
      key_term += expr.toString
      return key_term
    }
    else {
      var terms = new ListBuffer[String]()
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        var child = getJoinKeyTerms(expr.children(0), true)
        if (child != null)
          terms ++= child
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getJoinKeyTerms(expr.children(0), false)
        if (left_sub != null)
          terms ++= left_sub
        var right_sub = getJoinKeyTerms(expr.children(1), false)
        if (right_sub != null)
          terms ++= right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getJoinKeyTerms(expr.children(0), false)
        if (left_sub != null)
          terms ++= left_sub
        var right_sub = getJoinKeyTerms(expr.children(1), false)
        if (right_sub != null)
          terms ++= right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference"
          && expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference")
        {
          var left_sub = getJoinKeyTerms(expr.children(0), false)
          var right_sub = getJoinKeyTerms(expr.children(1), false)
          var this_term = ""
          if (negate)
            this_term = left_sub(0) + " != " + right_sub(0) // (expr.toString).replace(" = ", " != ")
          else
            this_term = left_sub(0) + " = " + right_sub(0) // expr.toString

          terms += this_term
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.isNull") {
        return null
      }
      else {
        println("Unsupported getJoinKeyTerms expression: " + expr.getClass.getName)
      }
      return terms
    }
  }

  def getJoinFilterTerms(expr: Expression, negate: Boolean): ListBuffer[String] = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return null
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      return null
    }
    else {
      var terms = new ListBuffer[String]()
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        var child = getJoinFilterTerms(expr.children(0), true)
        if (child != null)
          terms ++= child
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getJoinFilterTerms(expr.children(0), false)
        if (left_sub != null)
          terms ++= left_sub
        var right_sub = getJoinFilterTerms(expr.children(1), false)
        if (right_sub != null)
          terms ++= right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getJoinFilterTerms(expr.children(0), false)
        if (left_sub != null)
          terms ++= left_sub
        var right_sub = getJoinFilterTerms(expr.children(1), false)
        if (right_sub != null)
          terms ++= right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        if ((expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference"
          || expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference")
          && (expr.children(0).getClass.getName != "org.apache.spark.sql.catalyst.expressions.AttributeReference"
          || expr.children(1).getClass.getName != "org.apache.spark.sql.catalyst.expressions.AttributeReference")
        ) {
          if (negate)
            terms += (expr.toString).replace(" = ", " != ")
          else
            terms += expr.toString
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan") {
        if (negate)
          terms += (expr.toString).replace(" < ", " >= ")
        else
          terms += expr.toString
      }
      else {
        println("Unsupported getJoinFilterTerms expression: " + expr.getClass.getName)
      }
      return terms
    }
  }

  def getAggregateExpression(expr: Expression): String = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return stripColumnName(expr.toString)
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      if (expr.dataType.toString == "StringType"){
        return ( "\"" + expr.toString + "\"")
      }
      else {
        return expr.toString
      }
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Divide") {
        var left_div = getAggregateExpression(expr.children(0))
        var right_div = getAggregateExpression(expr.children(1))
        return "(" + left_div + " / " + right_div + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply") {
        var left_mul = getAggregateExpression(expr.children(0))
        var right_mul = getAggregateExpression(expr.children(1))
        return "(" + left_mul + " * " + right_mul + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add") {
        var left_add = getAggregateExpression(expr.children(0))
        var right_add = getAggregateExpression(expr.children(1))
        return "(" + left_add + " + " + right_add + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract") {
        var left_sub = getAggregateExpression(expr.children(0))
        var right_sub = getAggregateExpression(expr.children(1))
        return "(" + left_sub + " - " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        return "!" + "(" + getAggregateExpression(expr.children(0)) + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getAggregateExpression(expr.children(0))
        var right_sub = getAggregateExpression(expr.children(1))
        return "(" + left_sub + " && " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getAggregateExpression(expr.children(0))
        var right_sub = getAggregateExpression(expr.children(1))
        return "(" + left_sub + " || " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        var left_sub = getAggregateExpression(expr.children(0))
        var right_sub = getAggregateExpression(expr.children(1))
        if (expr.children(0).dataType.toString == "StringType") {
          return "(" + "std::string(" + left_sub + ".data())" + " == " + right_sub + ")"
        }
        else if (expr.children(1).dataType.toString == "StringType") {
          return "(" + left_sub + " == " + "std::string(" + right_sub + ".data())" + ")"
        }
        else {
          return "(" + left_sub + " == " + right_sub + ")"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
        var left_sub = getAggregateExpression(expr.children(0))
        var right_sub = getAggregateExpression(expr.children(1))
        var phrase_length = right_sub.length - 2 // -2 is to get rid of the length from the quotation characters "xxx"
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "=="  + "std::string(" + left_sub + ".data()).length() - " + phrase_length + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
        var left_sub = getAggregateExpression(expr.children(0))
        var right_sub = getAggregateExpression(expr.children(1))
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "=="  + "0" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
        var left_sub = getAggregateExpression(expr.children(0))
        var right_sub = getAggregateExpression(expr.children(1))
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "!="  + "std::string::npos" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring") {
        var target_str = getAggregateExpression(expr.children(0))
        var target_start_pos = (getAggregateExpression(expr.children(1)).toInt - 1).toString
        var target_str_len = getAggregateExpression(expr.children(2))
        return "std::string(" + target_str + ".data())" + ".substr(" + target_start_pos + ", " + target_str_len + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.CaseWhen") {
        var case_condition = getAggregateExpression(expr.children(0))
        var case_yes_val = getAggregateExpression(expr.children(1))
        var case_no_val = getAggregateExpression(expr.children(2))
        return case_condition + " ? " + case_yes_val + " : " + case_no_val
      }
      else {
        println(expr.getClass.getName)
        return getAggregateExpression(expr.children(0))
      }
    }
  }

  def getAggregateExpression_abstracted(expr: Expression, col_aggregate_ops: ListBuffer[Expression], start_op_idx: Int, prefix_str: String): String = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return stripColumnName(expr.toString)
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      if (expr.dataType.toString == "StringType"){
        return ( "\"" + expr.toString + "\"")
      }
      else {
        return expr.toString
      }
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Divide") {
        var left_div = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_div = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + left_div + " / " + right_div + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply") {
        var left_mul = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_mul = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + left_mul + " * " + right_mul + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add") {
        var left_add = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_add = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + left_add + " + " + right_add + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract") {
        var left_sub = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_sub = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + left_sub + " - " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        return "!" + "(" + getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str) + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_sub = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + left_sub + " && " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_sub = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + left_sub + " || " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        var left_sub = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_sub = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        if (expr.children(0).dataType.toString == "StringType") {
          return "(" + "std::string(" + left_sub + ".data())" + " == " + right_sub + ")"
        }
        else if (expr.children(1).dataType.toString == "StringType") {
          return "(" + left_sub + " == " + "std::string(" + right_sub + ".data())" + ")"
        }
        else {
          return "(" + left_sub + " == " + right_sub + ")"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
        var left_sub = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_sub = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        var phrase_length = right_sub.length - 2 // -2 is to get rid of the length from the quotation characters "xxx"
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "=="  + "std::string(" + left_sub + ".data()).length() - " + phrase_length + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
        var left_sub = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_sub = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "=="  + "0" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
        var left_sub = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var right_sub = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "!="  + "std::string::npos" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.CaseWhen") {
        var case_condition = getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
        var case_yes_val = getAggregateExpression_abstracted(expr.children(1), col_aggregate_ops, start_op_idx, prefix_str)
        var case_no_val = getAggregateExpression_abstracted(expr.children(2), col_aggregate_ops, start_op_idx, prefix_str)
        return case_condition + " ? " + case_yes_val + " : " + case_no_val
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Sum"){
        var op_idx = col_aggregate_ops.indexOf(expr) + start_op_idx
        return prefix_str + "sum_" + op_idx
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Count"){
        var op_idx = col_aggregate_ops.indexOf(expr) + start_op_idx
        return prefix_str + "count_" + op_idx
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Min"){
        var op_idx = col_aggregate_ops.indexOf(expr) + start_op_idx
        return prefix_str + "min_" + op_idx
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Max"){
        var op_idx = col_aggregate_ops.indexOf(expr) + start_op_idx
        return prefix_str + "max_" + op_idx
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Average"){
        var op_idx = col_aggregate_ops.indexOf(expr) + start_op_idx
        return prefix_str + "avg_" + op_idx + " / nrow1"
      }
      else {
        println(expr.getClass.getName)
        return getAggregateExpression_abstracted(expr.children(0), col_aggregate_ops, start_op_idx, prefix_str)
      }
    }
  }

  def getAggregateExpression_ALUOPCompiler(expr: Expression): String = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return expr.toString
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      if (expr.dataType.toString == "StringType"){
        return ( "\"" + expr.toString + "\"")
      }
      else {
        return expr.toString
      }
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Divide") {
        var left_div = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_div = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + left_div + " / " + right_div + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply") {
        var left_mul = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_mul = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + left_mul + " * " + right_mul + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add") {
        var left_add = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_add = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + left_add + " + " + right_add + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract") {
        var left_sub = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_sub = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + left_sub + " - " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        return "!" + "(" + getAggregateExpression_ALUOPCompiler(expr.children(0)) + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_sub = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + left_sub + " && " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_sub = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + left_sub + " || " + right_sub + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        var left_sub = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_sub = getAggregateExpression_ALUOPCompiler(expr.children(1))
        if (expr.children(0).dataType.toString == "StringType") {
          return "(" + "std::string(" + left_sub + ".data())" + " == " + right_sub + ")"
        }
        else if (expr.children(1).dataType.toString == "StringType") {
          return "(" + left_sub + " == " + "std::string(" + right_sub + ".data())" + ")"
        }
        else {
          return "(" + left_sub + " == " + right_sub + ")"
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
        var left_sub = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_sub = getAggregateExpression_ALUOPCompiler(expr.children(1))
        var phrase_length = right_sub.length - 2 // -2 is to get rid of the length from the quotation characters "xxx"
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "=="  + "std::string(" + left_sub + ".data()).length() - " + phrase_length + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
        var left_sub = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_sub = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "=="  + "0" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
        var left_sub = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var right_sub = getAggregateExpression_ALUOPCompiler(expr.children(1))
        return "(" + "std::string(" + left_sub + ".data()).find(" + right_sub + ")" + "!="  + "std::string::npos" + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring") {
        var target_str = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var target_start_pos = (getAggregateExpression_ALUOPCompiler(expr.children(1)).toInt - 1).toString
        var target_str_len = getAggregateExpression_ALUOPCompiler(expr.children(2))
        return "std::string(" + target_str + ".data())" + ".substr(" + target_start_pos + ", " + target_str_len + ")"
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.CaseWhen") {
        var case_condition = getAggregateExpression_ALUOPCompiler(expr.children(0))
        var case_yes_val = getAggregateExpression_ALUOPCompiler(expr.children(1))
        var case_no_val = getAggregateExpression_ALUOPCompiler(expr.children(2))
        return case_condition + " ? " + case_yes_val + " : " + case_no_val
      }
      else {
        println(expr.getClass.getName)
        return getAggregateExpression_ALUOPCompiler(expr.children(0))
      }
    }
  }

  def getAggregateExpression_ALUOPCompiler_cmd(expr: Expression, aggr_const: Array[Int], strm_order: Int): (String, Array[Int], Int) = {
    var this_aggr_const = new Array[Int](4)
    var this_strm_order = 0
    var this_aggr_const_right = new Array[Int](4)
    var this_strm_order_right = 0
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      var result = "strm" + strm_order.toString
      this_aggr_const = aggr_const
      this_strm_order = strm_order + 1
      return (result, this_aggr_const, this_strm_order)
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      var result = "c" + strm_order.toString
      this_aggr_const = aggr_const
      try {
        this_aggr_const(strm_order-1) = expr.toString.trim.toInt
      } catch {
        case e: NumberFormatException => this_aggr_const(strm_order-1) = 1
      }
      this_strm_order = strm_order + 1 // fixed
      return (result, this_aggr_const, this_strm_order)
    }
    else {
      var left_str = ""
      var right_str = ""
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply") {
        if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal" &&
          expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
          var (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), aggr_const, strm_order)
          var (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          return ("(" + left_str + "*" + right_str + ")", this_aggr_const, this_strm_order)
        }
        else if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" &&
          expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), aggr_const, strm_order)
          return ("(" + left_str + "*" + right_str + ")", this_aggr_const, this_strm_order)
        }
        else {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), this_aggr_const, this_strm_order)
          return ("(" + left_str + "*" + right_str + ")", this_aggr_const_right, this_strm_order_right)
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add") {
        if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal" &&
          expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          return ("(" + left_str + "+" + right_str + ")", this_aggr_const, this_strm_order)
        }
        else if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" &&
          expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), aggr_const, strm_order)
          return ("(" + left_str + "+" + right_str + ")", this_aggr_const, this_strm_order)
        }
        else {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), this_aggr_const, this_strm_order)
          return ("(" + left_str + "+" + right_str + ")", this_aggr_const_right, this_strm_order_right)
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract") {
        if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal" &&
          expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), aggr_const, strm_order)
          return ("(" + "-" + right_str + "+" + left_str +  ")", this_aggr_const, this_strm_order)
        } else if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" &&
          expr.children(1).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), aggr_const, strm_order)
          this_aggr_const(this_strm_order-1) = this_aggr_const(this_strm_order-1) * -1
          return ("(" + left_str + "+" + right_str + ")", this_aggr_const, this_strm_order)
        } else {
          val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
          val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), this_aggr_const, this_strm_order)
          return ("(" + left_str + "-" + right_str + ")", this_aggr_const_right, this_strm_order_right)
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
        val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), this_aggr_const, this_strm_order)
        return ("(" + left_str + " && " + right_str + ")", this_aggr_const_right, this_strm_order_right)
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
        val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), this_aggr_const, this_strm_order)
        return ("(" + left_str + " || " + right_str + ")", this_aggr_const_right, this_strm_order_right)
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        val (left_str, this_aggr_const, this_strm_order) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
        val (right_str, this_aggr_const_right, this_strm_order_right) = getAggregateExpression_ALUOPCompiler_cmd(expr.children(1), this_aggr_const, this_strm_order)
        return ("(" + left_str + " == " + right_str + ")", this_aggr_const_right, this_strm_order_right)
      }
      else {
        println(expr.getClass.getName)
        return getAggregateExpression_ALUOPCompiler_cmd(expr.children(0), aggr_const, strm_order)
      }
    }
  }

  def getAggregateOperations (expr: Expression): ListBuffer[Expression] = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      var tmp_aggregation_op = new ListBuffer[Expression]()
      return tmp_aggregation_op
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      var tmp_aggregation_op = new ListBuffer[Expression]()
      return tmp_aggregation_op
    }
    else {
      var this_op = new ListBuffer[Expression]()
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression"){
        this_op += expr.children(0)
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Sum"){
        this_op += expr
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Count"){
        this_op += expr
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Min"){
        this_op += expr
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Max"){
        this_op += expr
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Avg"){
        this_op += expr
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Divide") {
        var left_div = getAggregateOperations(expr.children(0))
        var right_div = getAggregateOperations(expr.children(1))
        this_op = left_div ++ right_div
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply") {
        var left_mul = getAggregateOperations(expr.children(0))
        var right_mul = getAggregateOperations(expr.children(1))
        this_op = left_mul ++ right_mul
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add") {
        var left_add = getAggregateOperations(expr.children(0))
        var right_add = getAggregateOperations(expr.children(1))
        this_op = left_add ++ right_add
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract") {
        var left_sub = getAggregateOperations(expr.children(0))
        var right_sub = getAggregateOperations(expr.children(1))
        this_op = left_sub ++ right_sub
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        this_op = getAggregateOperations(expr.children(0))
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_and = getAggregateOperations(expr.children(0))
        var right_and = getAggregateOperations(expr.children(1))
        this_op = left_and ++ right_and
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_or = getAggregateOperations(expr.children(0))
        var right_or = getAggregateOperations(expr.children(1))
        this_op = left_or ++ right_or
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        var left_equal = getAggregateOperations(expr.children(0))
        var right_equal = getAggregateOperations(expr.children(1))
        this_op = left_equal ++ right_equal
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
        var left_end = getAggregateOperations(expr.children(0))
        var right_end = getAggregateOperations(expr.children(1))
        this_op = left_end ++ right_end
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
        var left_start = getAggregateOperations(expr.children(0))
        var right_start = getAggregateOperations(expr.children(1))
        this_op = left_start ++ right_start
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
        var left_contain = getAggregateOperations(expr.children(0))
        var right_contain = getAggregateOperations(expr.children(1))
        this_op = left_contain ++ right_contain
        return this_op
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.CaseWhen") {
        var case_condition = getAggregateOperations(expr.children(0))
        var case_yes_val = getAggregateOperations(expr.children(1))
        var case_no_val = getAggregateOperations(expr.children(2))
        this_op = case_condition ++ case_yes_val ++ case_no_val
        return this_op
      }
      else {
        println(expr.getClass.getName)
        return getAggregateOperations(expr.children(0))
      }
    }
  }

  def getAggregateOpEnum(expr: Expression): String = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Alias") {
      return getAggregateOpEnum(expr.children(0))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression") {
      return getAggregateOpEnum(expr.children(0))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Sum") {
      return "xf::database::enums::AOP_SUM"
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Count"){
      // fix: add support for "xf::database::enums::AOP_COUNTNONZEROS"
      if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
        return "xf::database::enums::AOP_COUNTNONZEROS"
      }
      else if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
        return "xf::database::enums::AOP_COUNT"
      }
      else {
        return "xf::database::enums::AOP_COUNT"
      }
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Min"){
      return "xf::database::enums::AOP_MIN"
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Max"){
      return "xf::database::enums::AOP_MAX"
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Average"){
      return "xf::database::enums::AOP_MEAN"
    }
    else {
      return "xf::database::enums::AOP_SUM" //default aggr_op enum
    }
  }

  def isValidAggregateExpression(expr: Expression): Boolean = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return true
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      if (expr.dataType.toString == "StringType"){
        return false
      } else {
        return true
      }
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Divide") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply") {
        var left_mul = isValidAggregateExpression(expr.children(0))
        var right_mul = isValidAggregateExpression(expr.children(1))
        return left_mul & right_mul
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add") {
        var left_add = isValidAggregateExpression(expr.children(0))
        var right_add = isValidAggregateExpression(expr.children(1))
        return left_add & right_add
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract") {
        var left_sub = isValidAggregateExpression(expr.children(0))
        var right_sub = isValidAggregateExpression(expr.children(1))
        return left_sub & right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
        return isValidAggregateExpression(expr.children(0))
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = isValidAggregateExpression(expr.children(0))
        var right_sub = isValidAggregateExpression(expr.children(1))
        return left_sub & right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        var left_sub = isValidAggregateExpression(expr.children(0))
        var right_sub = isValidAggregateExpression(expr.children(1))
        return left_sub & right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
        var left_sub = isValidAggregateExpression(expr.children(0))
        var right_sub = isValidAggregateExpression(expr.children(1))
        if (expr.children(0).dataType.toString == "StringType" ||
          expr.children(1).dataType.toString == "StringType") {
          return false
        } else {
          return left_sub & right_sub
        }
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.CaseWhen") {
        return false
      }
      else {
        println(expr.getClass.getName)
        return isValidAggregateExpression(expr.children(0))
      }
    }
  }

  def isPureAggrOperation_sub(expr: Expression): Boolean = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Alias") {
      return isPureAggrOperation_sub(expr.children(0))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression") {
      return isPureAggrOperation_sub(expr.children(0))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Sum" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Count" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Min" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Max" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.Average") {
      if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" ||
        expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
        return true
      } else {
        return false
      }
    }
    else {
      println(expr.getClass.getName)
      return false
    }
  }

  def isPureEvalOperation_sub(expr: Expression): Boolean = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference") {
      return true
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      return true
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Alias") {
      return isPureEvalOperation_sub(expr.children(0))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Divide") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply") {
      var left_mul = isPureEvalOperation_sub(expr.children(0))
      var right_mul = isPureEvalOperation_sub(expr.children(1))
      return left_mul & right_mul
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add") {
      var left_add = isPureEvalOperation_sub(expr.children(0))
      var right_add = isPureEvalOperation_sub(expr.children(1))
      return left_add & right_add
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract") {
      var left_sub = isPureEvalOperation_sub(expr.children(0))
      var right_sub = isPureEvalOperation_sub(expr.children(1))
      return left_sub & right_sub
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not") {
      return isPureEvalOperation_sub(expr.children(0))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
      var left_sub = isPureEvalOperation_sub(expr.children(0))
      var right_sub = isPureEvalOperation_sub(expr.children(1))
      return left_sub & right_sub
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
      var left_sub = isPureEvalOperation_sub(expr.children(0))
      var right_sub = isPureEvalOperation_sub(expr.children(1))
      return left_sub & right_sub
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo") {
      var left_sub = isPureEvalOperation_sub(expr.children(0))
      var right_sub = isPureEvalOperation_sub(expr.children(1))
      return left_sub & right_sub
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.CaseWhen") {
      return false
    }
    else {
      println(expr.getClass.getName)
      return isPureEvalOperation_sub(expr.children(0))
    }
  }

  def isPureAggrOperation(expr_list: ListBuffer[Expression]): Boolean = {
    for (expr <- expr_list) {
      if (!isPureAggrOperation_sub(expr)) {
        return false
      }
    }
    return true
  }

  def isPureEvalOperation(expr_list: ListBuffer[Expression]): Boolean = {
    for (expr <- expr_list) {
      if (!isPureEvalOperation_sub(expr)) {
        return false
      }
    }
    return true
  }

  def getNumberofFilterClause(expr: Expression): Int = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.IsNotNull") {
      return 0
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or"
      ) {
        var left_sub = getNumberofFilterClause(expr.children(0))
        var right_sub = getNumberofFilterClause(expr.children(1))
        return left_sub + right_sub
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThan" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan"
      ) {
        return 1
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Like"||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.In" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.ScalarSubquery"
      ) {
        return 0
      }
      else {
        return getNumberofFilterClause(expr.children(0))
      }
    }
  }

  def isAllFilterClausesValid(expr: Expression): Boolean = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.IsNotNull") {
      return true
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = isAllFilterClausesValid(expr.children(0))
        var right_sub = isAllFilterClausesValid(expr.children(1))
        return left_sub & right_sub
      }
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThan" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan") {
        return true
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Like"||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.In" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.ScalarSubquery"
      ) {
        return false
      }
      else {
        return isAllFilterClausesValid(expr.children(0))
      }
    }
  }

  def getFilterClause(expr: Expression): ListBuffer[String] = {
    var filter_clauses = new ListBuffer[String]

    //      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" ||
    //        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
    //        return false
    //      }
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.IsNotNull") {
      return filter_clauses
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
      var left_sub = getFilterClause(expr.children(0))
      var right_sub = getFilterClause(expr.children(1))
      filter_clauses ++= left_sub
      filter_clauses ++= right_sub
      return filter_clauses
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThan" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan") {
      var className: String = expr.getClass.getName
      className match {
        case "org.apache.spark.sql.catalyst.expressions.EqualTo" =>
          filter_clauses += expr.children(0).toString + " = " + expr.children(1).toString
        case "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual" =>
          filter_clauses += expr.children(0).toString + " >= " + expr.children(1).toString
        case "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual" =>
          filter_clauses += expr.children(0).toString + " <= " + expr.children(1).toString
        case "org.apache.spark.sql.catalyst.expressions.GreaterThan" =>
          filter_clauses += expr.children(0).toString + " > " + expr.children(1).toString
        case "org.apache.spark.sql.catalyst.expressions.LessThan" =>
          filter_clauses += expr.children(0).toString + " < " + expr.children(1).toString
        case _ =>
          filter_clauses += "Error"
      }
      return filter_clauses
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Like"||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.In" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.ScalarSubquery") {
      return filter_clauses
    }
    else {
      return getFilterClause(expr.children(0))
    }
  }

  def isAllFilterClausesIsNotNull(expr: Expression): Boolean = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference" ||
      expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Literal") {
      return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.IsNotNull") {
      return true
    }
    else {
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And") {
        var left_sub = isAllFilterClausesIsNotNull(expr.children(0))
        var right_sub = isAllFilterClausesIsNotNull(expr.children(1))
        return left_sub & right_sub
      }
      if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThanOrEqual" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.GreaterThan" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.LessThan") {
        return false
      }
      else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Like"||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.In" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring" ||
        expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.ScalarSubquery"
      ) {
        return false
      }
      else {
        return isAllFilterClausesIsNotNull(expr.children(0))
      }
    }
  }

  def isOnlyContainsAliasProjection(expr: Expression): Boolean = {
    if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Alias") {
      if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.AttributeReference")
        return true
      else
        return false
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression") {
      return isOnlyContainsAliasProjection(expr.children(0))
    }
    else if (expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Divide"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Multiply"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Add"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Subtract"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Not"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.And"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Or"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EqualTo"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.EndsWith"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.StartsWith"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Contains"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring"
      || expr.getClass.getName == "org.apache.spark.sql.catalyst.expressions.CaseWhen") {
      return false
    }
    else {
      return isOnlyContainsAliasProjection(expr.children(0))
    }
  }

  def genJoin_KeyPayloadStruct(thisNode: SQL2FPGA_QPlan, tableOrder: String, dfmap: Map[String, DataFrame]): (ListBuffer[String], ListBuffer[String], ListBuffer[String], ListBuffer[String], ListBuffer[String], String, String) = {
    var structCode = new ListBuffer[String]()
    var leftTableKeyColNames = new ListBuffer[String]()
    var rightTableKeyColNames = new ListBuffer[String]()
    var leftTablePayloadColNames = new ListBuffer[String]()
    var rightTablePayloadColNames = new ListBuffer[String]()
    var joinKeyTypeName = thisNode._fpgaSWFuncName + "_key_" + tableOrder
    var joinPayloadTypeName = thisNode._fpgaSWFuncName + "_payload_" + tableOrder

    //Default - leftMajor
    var join_left_table_col = thisNode._children.head.outputCols
    var join_right_table_col = thisNode._children.last.outputCols
    var left_child = thisNode._children.head
    var right_child = thisNode._children.last

    tableOrder match {
      case "leftMajor" =>
        join_left_table_col = thisNode._children.head.outputCols
        join_right_table_col = thisNode._children.last.outputCols
        left_child = thisNode._children.head
        right_child = thisNode._children.last
      case "rightMajor" =>
        join_left_table_col = thisNode._children.last.outputCols
        join_right_table_col = thisNode._children.head.outputCols
        left_child = thisNode._children.last
        right_child = thisNode._children.head
      case _ =>
    }
    var join_key_pairs = getJoinKeyTerms(thisNode._joining_expression(0), false)

    //Key struct
    structCode += "struct " + joinKeyTypeName + " {"
    for (key_pair <- join_key_pairs) {
      var leftTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" ").head
      var rightTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" ").last
      if (join_left_table_col.indexOf(leftTblCol) == -1) {
        var tmpTblCol = leftTblCol
        leftTblCol = rightTblCol
        rightTblCol = tmpTblCol
      }
      leftTableKeyColNames += leftTblCol
      rightTableKeyColNames += rightTblCol
      var leftTblColType = getColumnType(leftTblCol, dfmap)
      var leftTblColName = stripColumnName(leftTblCol)
      leftTblColType match {
        case "IntegerType" =>
          structCode += "    int32_t " + leftTblColName + ";"
        case "LongType" =>
          structCode += "    int64_t " + leftTblColName + ";"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            structCode += "    int32_t " + leftTblColName + ";"
          } else {
            structCode += "    std::string " + leftTblColName + ";"
          }
        case _ =>
          structCode += "    // Unsupported join key type"
      }
    }
    structCode += "    bool operator==(const " + joinKeyTypeName + "& other) const {"
    var equivalentOperation = "        return ("
    var i = 0
    for (i <- 0 to leftTableKeyColNames.length - 2) {
      var equality = join_key_pairs(i).stripPrefix("(").stripSuffix(")").trim.split(" ")(1) // operator is in index 1
      if (equality == "=") {
        equivalentOperation += "(" + stripColumnName(leftTableKeyColNames(i)) + " == other." + stripColumnName(leftTableKeyColNames(i)) + ") && "
      } else if (equality == "!=") {
        equivalentOperation += "(" + stripColumnName(leftTableKeyColNames(i)) + " != other." + stripColumnName(leftTableKeyColNames(i)) + ") && "
      }
    }
    var equality = join_key_pairs.last.stripPrefix("(").stripSuffix(")").trim.split(" ")(1) // operator is in index 1
    if (equality == "=") {
      equivalentOperation += "(" + stripColumnName(leftTableKeyColNames.last) + " == other." + stripColumnName(leftTableKeyColNames.last) + "));"
    } else if (equality == "!=") {
      equivalentOperation += "(" + stripColumnName(leftTableKeyColNames.last) + " != other." + stripColumnName(leftTableKeyColNames.last) + "));"
    }
    structCode += equivalentOperation
    structCode += "    }"
    structCode += "};"

    //Key hash struct - joinKeyHashTypeName
    structCode += "namespace std {"
    structCode += "template <>"
    structCode += "struct hash<" + joinKeyTypeName + "> {"
    structCode += "    std::size_t operator() (const " + joinKeyTypeName + "& k) const {"
    structCode += "        using std::size_t;"
    structCode += "        using std::hash;"
    structCode += "        using std::string;"
    var joinKeyHashStr = "        return "
    i = 0
    for (left_key_pair <- leftTableKeyColNames) {
      var equality = join_key_pairs(i).stripPrefix("(").stripSuffix(")").trim.split(" ")(1) // operator is in index 1
      if (equality == "=") {
        var leftTblColType = getColumnType(left_key_pair, dfmap)
        var leftTblColName = stripColumnName(left_key_pair)
        leftTblColType match {
          case "IntegerType" =>
            joinKeyHashStr += "(hash<int32_t>()(k." + leftTblColName + ")) + "
          case "LongType" =>
            joinKeyHashStr += "(hash<int64_t>()(k." + leftTblColName + ")) + "
          case "StringType" =>
            if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
              joinKeyHashStr += "(hash<int32_t>()(k." + leftTblColName + ")) + "
            } else {
              joinKeyHashStr += "(hash<string>()(k." + leftTblColName + ")) + "
            }
          case _ =>
            structCode += "        // Unsupported join key type"
        }
      }
      i += 1
    }
    structCode += joinKeyHashStr.stripSuffix(" + ") + ";" // removes the '+' from the last term
    structCode += "    }"
    structCode += "};"
    structCode += "}"

    //Payload struct - Left table
    structCode += "struct " + joinPayloadTypeName + " {"
    for (left_payload <- join_left_table_col) {
      var leftTblColName = stripColumnName(left_payload)
      var leftPayloadColType = getColumnType(left_payload, dfmap)
      leftPayloadColType match {
        case "IntegerType" =>
          structCode += "    int32_t " + leftTblColName + ";"
        case "LongType" =>
          structCode += "    int64_t " + leftTblColName + ";"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            structCode += "    int32_t " + leftTblColName + ";"
          } else {
            structCode += "    std::string " + leftTblColName + ";"
          }
        case _ =>
          structCode += "    // Unsupported join key type"
      }
      leftTablePayloadColNames += left_payload
    }
    structCode += "};"
    //Payload struct - Right table
    for (right_payload <- join_right_table_col) {
      rightTablePayloadColNames += right_payload
    }

    return (structCode,
      leftTableKeyColNames, rightTableKeyColNames, leftTablePayloadColNames, rightTablePayloadColNames,
      joinKeyTypeName, joinPayloadTypeName)
  }

  def genAntiJoin_leftMajor_core(thisNode: SQL2FPGA_QPlan, dfmap: Map[String, DataFrame],
                                 leftTableKeyColNames: ListBuffer[String], rightTableKeyColNames: ListBuffer[String],
                                 leftTablePayloadColNames: ListBuffer[String], rightTablePayloadColNames: ListBuffer[String],
                                 joinKeyTypeName: String, joinPayloadTypeName: String): ListBuffer[String] = {
    var coreCode = new ListBuffer[String]()

    var tbl_in_1 = thisNode._children.head.fpgaOutputTableName
    var tbl_in_2 = thisNode._children.last.fpgaOutputTableName
    var tbl_out_1 = thisNode._fpgaOutputTableName
    var join_left_table_col = thisNode._children.head.outputCols
    var join_right_table_col = thisNode._children.last.outputCols
    var left_child = thisNode._children.head
    var right_child = thisNode._children.last

    coreCode += "    std::unordered_multimap<" + joinKeyTypeName + ", " + joinPayloadTypeName + "> ht1;"
    coreCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
    coreCode += "    int nrow2 = " + tbl_in_2 + ".getNumRow();"
    // Enumerate input table
    coreCode += "    for (int i = 0; i < nrow1; i++) {"
    //  Key
    var key_str = ""
    for (key_col <- leftTableKeyColNames) {
      var join_left_key_col_name = stripColumnName(key_col) + "_k"
      var join_left_key_col_type = getColumnType(key_col, dfmap)
      var join_left_key_col_idx = join_left_table_col.indexOf(key_col)
      join_left_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_key_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
            coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_left_key_col_name + ", "
    }
    coreCode += "        " + joinKeyTypeName + " keyA{" + key_str.stripSuffix(", ") + "};"
    //  Payload
    var payload_str = ""
    for (payload_col <- leftTablePayloadColNames) {
      var join_left_payload_col_name = stripColumnName(payload_col)
      var join_left_payload_col_type = getColumnType(payload_col, dfmap)
      var join_left_payload_col_idx = join_left_table_col.indexOf(payload_col)
      join_left_payload_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_payload_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(payload_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
            coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      payload_str += join_left_payload_col_name + ", "
    }
    coreCode += "        " + joinPayloadTypeName + " payloadA{" + payload_str.stripSuffix(", ") + "};"
    coreCode += "        ht1.insert(std::make_pair(keyA, payloadA));"
    coreCode += "    }"

    // Enumerate output table
    coreCode += "    for (int i = 0; i < nrow2; i++) {"
    //  Key
    key_str = ""
    for (key_col <- rightTableKeyColNames) {
      var join_right_key_col_name = stripColumnName(key_col) + "_k"
      var join_right_key_col_type = getColumnType(key_col, dfmap)
      var join_right_key_col_idx = join_right_table_col.indexOf(key_col)
      join_right_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt64(i, " + join_right_key_col_idx + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
          } else if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
            coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_right_key_col_name + ", "
    }
    coreCode += "        ht1.erase(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "}" + ");"
    coreCode += "    }"

    coreCode += "    int r = 0;"
    coreCode += "    for (auto iter=ht1.begin(); iter!=ht1.end(); iter=ht1.equal_range(iter->first).second) {"
    coreCode += "        auto unique_key = iter->first;"
    coreCode += "        if (ht1.count(unique_key) >= 1){"
    coreCode += "            auto it_bounds = ht1.equal_range(unique_key);"
    coreCode += "            for (auto it=it_bounds.first; it!=it_bounds.second; it++) {"
    for (left_payload <- leftTablePayloadColNames) {
      var left_payload_name = stripColumnName(left_payload)
      var left_payload_type = getColumnType(left_payload, dfmap)
      var left_payload_input_index = join_left_table_col.indexOf(left_payload)
      left_payload_type match {
        case "IntegerType" =>
          coreCode += "                int32_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
        case "LongType" =>
          coreCode += "                int64_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "                int32_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
          } else {
            coreCode += "                std::string " + left_payload_name + " = (it->second)." + left_payload_name + ";"
            coreCode += "                std::array<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1> " + left_payload_name + "_n" + "{};"
            coreCode += "                memcpy(" + left_payload_name + "_n" + ".data(), (" + left_payload_name + ").data(), (" + left_payload_name + ").length());"
          }
        case _ =>
          coreCode += "                // Unsupported join key type"
      }
    }
    var join_filter_pairs = getJoinFilterTerms(thisNode.joining_expression(0), false)
    if (join_filter_pairs.length > 0) { //filtering is need
      var filter_expr = getJoinFilterExpression(joining_expression(0))
      coreCode += "            if " + filter_expr + " {"
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "                    " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "                    " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                    " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "                    " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "                    // Unsupported join key type"
          }
        }
      }
      coreCode += "                    r++;"
      coreCode += "                }"
    }
    else { //no filtering - standard write out output columns
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "                " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "                // Unsupported join key type"
          }
        }
      }
      coreCode += "                r++;"
    }
    coreCode += "            }"
    coreCode += "        }"
    coreCode += "    }"
  }

  def genAntiJoin_rightMajor_core(thisNode: SQL2FPGA_QPlan, dfmap: Map[String, DataFrame],
                                  leftTableKeyColNames: ListBuffer[String], rightTableKeyColNames: ListBuffer[String],
                                  leftTablePayloadColNames: ListBuffer[String], rightTablePayloadColNames: ListBuffer[String],
                                  joinKeyTypeName: String, joinPayloadTypeName: String): ListBuffer[String] = {
    var coreCode = new ListBuffer[String]()

    var tbl_in_1 = thisNode._children.last.fpgaOutputTableName
    var tbl_in_2 = thisNode._children.head.fpgaOutputTableName
    var tbl_out_1 = thisNode._fpgaOutputTableName
    var join_left_table_col = thisNode._children.last.outputCols
    var join_right_table_col = thisNode._children.head.outputCols
    var left_child = thisNode._children.last
    var right_child = thisNode._children.head

    coreCode += "    std::unordered_map<" + joinKeyTypeName + ", " + joinPayloadTypeName + "> ht1;"
    coreCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
    coreCode += "    int nrow2 = " + tbl_in_2 + ".getNumRow();"
    // Enumerate input table
    coreCode += "    for (int i = 0; i < nrow1; i++) {"
    //  Key
    var key_str = ""
    for (key_col <- leftTableKeyColNames) {
      var join_left_key_col_name = stripColumnName(key_col) + "_k"
      var join_left_key_col_type = getColumnType(key_col, dfmap)
      var join_left_key_col_idx = join_left_table_col.indexOf(key_col)
      join_left_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_key_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
            coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_left_key_col_name + ", "
    }
    coreCode += "        " + joinKeyTypeName + " keyA{" + key_str.stripSuffix(", ") + "};"
    //  Payload
    var payload_str = ""
    for (payload_col <- leftTablePayloadColNames) {
      var join_left_payload_col_name = stripColumnName(payload_col)
      var join_left_payload_col_type = getColumnType(payload_col, dfmap)
      var join_left_payload_col_idx = join_left_table_col.indexOf(payload_col)
      join_left_payload_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_payload_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(payload_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
            coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      payload_str += join_left_payload_col_name + ", "
    }
    coreCode += "        " + joinPayloadTypeName + " payloadA{" + payload_str.stripSuffix(", ") + "};"
    coreCode += "        ht1.insert(std::make_pair(keyA, payloadA));"
    coreCode += "    }"
    coreCode += "    int r = 0;"
    // Enumerate output table
    coreCode += "    for (int i = 0; i < nrow2; i++) {"
    //  Key
    key_str = ""
    for (key_col <- rightTableKeyColNames) {
      var join_right_key_col_name = stripColumnName(key_col) + "_k"
      var join_right_key_col_type = getColumnType(key_col, dfmap)
      var join_right_key_col_idx = join_right_table_col.indexOf(key_col)
      join_right_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt64(i, " + join_right_key_col_idx + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
          } else if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
            coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_right_key_col_name + ", "
    }

    coreCode += "        auto it = ht1.find(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "}" + ");"
    coreCode += "        if (it == ht1.end()) {"
    for (right_payload <- rightTablePayloadColNames) {
      var right_payload_name = stripColumnName(right_payload)
      var right_payload_type = getColumnType(right_payload, dfmap)
      var right_payload_input_index = join_right_table_col.indexOf(right_payload)
      var right_payload_index = thisNode._outputCols.indexOf(right_payload)
      right_payload_type match {
        case "IntegerType" =>
          coreCode += "            int32_t " + right_payload_name + " = " + tbl_in_2 + ".getInt32(i, " + right_payload_input_index + ");"
        case "LongType" =>
          coreCode += "            int64_t " + right_payload_name + " = " + tbl_in_2 + ".getInt64(i, " + right_payload_input_index + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "            int32_t " + right_payload_name + " = " + tbl_in_2 + ".getInt32(i, " + right_payload_input_index + ");"
          } else if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(right_payload) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + "_n" + " = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(i, " + right_payload_input_index + ");"
              coreCode += "            std::string " + right_payload_name + " = std::string(" + right_payload_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_2 + ".getInt32(i, " + right_payload_input_index + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + right_payload_name + " = std::string(" + right_payload_name + "_n.data());"
            }
          } else {
            coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + " = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(i, " + right_payload_input_index + ");"
          }
        case _ =>
          coreCode += "            // Unsupported join key type"
      }
    }
    var join_filter_pairs = getJoinFilterTerms(thisNode.joining_expression(0), false)
    if (join_filter_pairs.length > 0) { //filtering is need
      var filter_expr = getJoinFilterExpression(joining_expression(0))
      coreCode += "            if " + filter_expr + " {"
      var idx = 0
      for (right_payload <- rightTablePayloadColNames) {
        var right_payload_name = stripColumnName(right_payload)
        var right_payload_type = getColumnType(right_payload, dfmap)
        var right_payload_input_index = join_right_table_col.indexOf(right_payload)
        var right_payload_index = thisNode._outputCols.indexOf(right_payload)
        if (right_payload_index != -1) {
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "                " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
              } else {
                coreCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + ");"
              }
            case _ =>
              coreCode += "                // Unsupported join key type"
          }
        }
        else if (thisNode._isSpecialAntiJoin) { //TODO: fix hard-coded index
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + idx + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + idx + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + idx + ", " + right_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + idx + ", " + right_payload_name + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
          idx += 1
        }
      }
      coreCode += "                r++;"
      coreCode += "            }"
    }
    else { //no filtering - standard write out output columns
      var idx = 0
      for (right_payload <- rightTablePayloadColNames) {
        var right_payload_name = stripColumnName(right_payload)
        var right_payload_type = getColumnType(right_payload, dfmap)
        var right_payload_input_index = join_right_table_col.indexOf(right_payload)
        var right_payload_index = thisNode._outputCols.indexOf(right_payload)
        if (right_payload_index != -1) {
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
        }
        else if (thisNode._isSpecialAntiJoin) { //TODO: fix hard-coded index
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + idx + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + idx + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + idx + ", " + right_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + idx + ", " + right_payload_name + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
          idx += 1
        }
      }
      coreCode += "            r++;"
    }
    coreCode += "        }"
    coreCode += "    }"
    coreCode += "    " + tbl_out_1 + ".setNumRow(r);"
  }

  def genSemiJoin_leftMajor_core(thisNode: SQL2FPGA_QPlan, dfmap: Map[String, DataFrame],
                                 leftTableKeyColNames: ListBuffer[String], rightTableKeyColNames: ListBuffer[String],
                                 leftTablePayloadColNames: ListBuffer[String], rightTablePayloadColNames: ListBuffer[String],
                                 joinKeyTypeName: String, joinPayloadTypeName: String): ListBuffer[String] = {
    var coreCode = new ListBuffer[String]()

    var tbl_in_1 = thisNode._children.head.fpgaOutputTableName
    var tbl_in_2 = thisNode._children.last.fpgaOutputTableName
    var join_left_table_col = thisNode._children.head.outputCols
    var join_right_table_col = thisNode._children.last.outputCols
    var tbl_out_1 = thisNode._fpgaOutputTableName
    var left_child = thisNode._children.head
    var right_child = thisNode._children.last

    coreCode += "    std::unordered_multimap<" + joinKeyTypeName + ", " + joinPayloadTypeName + "> ht1;"
    coreCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
    coreCode += "    int nrow2 = " + tbl_in_2 + ".getNumRow();"
    // Enumerate input table
    coreCode += "    for (int i = 0; i < nrow1; i++) {"
    //  Key
    var key_str = ""
    for (key_col <- leftTableKeyColNames) {
      var join_left_key_col_name = stripColumnName(key_col) + "_k"
      var join_left_key_col_type = getColumnType(key_col, dfmap)
      var join_left_key_col_idx = join_left_table_col.indexOf(key_col)
      join_left_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_key_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            } else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
            coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_left_key_col_name + ", "
    }
    coreCode += "        " + joinKeyTypeName + " keyA{" + key_str.stripSuffix(", ") + "};"
    //  Payload
    var payload_str = ""
    for (payload_col <- leftTablePayloadColNames) {
      var join_left_payload_col_name = stripColumnName(payload_col)
      var join_left_payload_col_type = getColumnType(payload_col, dfmap)
      var join_left_payload_col_idx = join_left_table_col.indexOf(payload_col)
      join_left_payload_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_payload_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(payload_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
            coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      payload_str += join_left_payload_col_name + ", "
    }
    coreCode += "        " + joinPayloadTypeName + " payloadA{" + payload_str.stripSuffix(", ") + "};"
    coreCode += "        ht1.insert(std::make_pair(keyA, payloadA));"
    coreCode += "    }"
    coreCode += "    int r = 0;"
    // Enumerate output table
    coreCode += "    for (int i = 0; i < nrow2; i++) {"
    //  Key
    key_str = ""
    for (key_col <- rightTableKeyColNames) {
      var join_right_key_col_name = stripColumnName(key_col) + "_k"
      var join_right_key_col_type = getColumnType(key_col, dfmap)
      var join_right_key_col_idx = join_right_table_col.indexOf(key_col)
      join_right_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt64(i, " + join_right_key_col_idx + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
            coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_right_key_col_name + ", "
    }
    coreCode += "        auto its = ht1.equal_range(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "});"
    coreCode += "        auto it = its.first;"
    coreCode += "        if (it == its.second) {"
    coreCode += "            continue;"
    coreCode += "        }"
    coreCode += "        else {"
    coreCode += "        while (it != its.second) {"
    //  Payload
    for (left_payload <- leftTablePayloadColNames) {
      var left_payload_name = stripColumnName(left_payload)
      var left_payload_type = getColumnType(left_payload, dfmap)
      left_payload_type match {
        case "IntegerType" =>
          coreCode += "            int32_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
        case "LongType" =>
          coreCode += "            int64_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "            int32_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
          } else {
            coreCode += "            std::string " + left_payload_name + " = (it->second)." + left_payload_name + ";"
            coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1> " + left_payload_name + "_n" + "{};"
            coreCode += "            memcpy(" + left_payload_name + "_n" + ".data(), (" + left_payload_name + ").data(), (" + left_payload_name + ").length());"
          }
        case _ =>
          coreCode += "            // Unsupported join key type"
      }
    }
    var join_filter_pairs = getJoinFilterTerms(thisNode._joining_expression(0), false)
    if (join_filter_pairs.length > 0) { //filtering is need
      var filter_expr = getJoinFilterExpression(thisNode.joining_expression(0))
      coreCode += "            if " + filter_expr + " {"
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "                " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "                // Unsupported join key type"
          }
        }
      }
      coreCode += "                r++;"
      coreCode += "            }"
      coreCode += "            it++;"
    }
    else { //no filtering - standard write out output columns
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
        }
      }
      coreCode += "            it++;"
      coreCode += "            r++;"
    }
    coreCode += "        }"
    coreCode += "            ht1.erase(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "});"
    coreCode += "        }"
    coreCode += "    }"
    coreCode += "    " + tbl_out_1 + ".setNumRow(r);"

    return coreCode
  }

  def genSemiJoin_rightMajor_core(thisNode: SQL2FPGA_QPlan, dfmap: Map[String, DataFrame],
                                  leftTableKeyColNames: ListBuffer[String], rightTableKeyColNames: ListBuffer[String],
                                  leftTablePayloadColNames: ListBuffer[String], rightTablePayloadColNames: ListBuffer[String],
                                  joinKeyTypeName: String, joinPayloadTypeName: String): ListBuffer[String] = {
    var coreCode = new ListBuffer[String]()

    var tbl_in_1 = thisNode._children.last.fpgaOutputTableName
    var tbl_in_2 = thisNode._children.head.fpgaOutputTableName
    var tbl_out_1 = thisNode._fpgaOutputTableName
    var join_left_table_col = thisNode._children.last.outputCols
    var join_right_table_col = thisNode._children.head.outputCols
    var left_child = thisNode._children.last
    var right_child = thisNode._children.head

    coreCode += "    std::unordered_map<" + joinKeyTypeName + ", " + joinPayloadTypeName + "> ht1;"
    coreCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
    coreCode += "    int nrow2 = " + tbl_in_2 + ".getNumRow();"
    // Enumerate input table
    coreCode += "    for (int i = 0; i < nrow1; i++) {"
    //  Key
    var key_str = ""
    for (key_col <- leftTableKeyColNames) {
      var join_left_key_col_name = stripColumnName(key_col) + "_k"
      var join_left_key_col_type = getColumnType(key_col, dfmap)
      var join_left_key_col_idx = join_left_table_col.indexOf(key_col)
      join_left_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_key_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
            coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_left_key_col_name + ", "
    }
    coreCode += "        " + joinKeyTypeName + " keyA{" + key_str.stripSuffix(", ") + "};"
    //  Payload
    var payload_str = ""
    for (payload_col <- leftTablePayloadColNames) {
      var join_left_payload_col_name = stripColumnName(payload_col)
      var join_left_payload_col_type = getColumnType(payload_col, dfmap)
      var join_left_payload_col_idx = join_left_table_col.indexOf(payload_col)
      join_left_payload_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_payload_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
          } else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(payload_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
            coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      payload_str += join_left_payload_col_name + ", "
    }
    coreCode += "        " + joinPayloadTypeName + " payloadA{" + payload_str.stripSuffix(", ") + "};"
    coreCode += "        ht1.insert(std::make_pair(keyA, payloadA));"
    coreCode += "    }"
    coreCode += "    int r = 0;"
    // Enumerate output table
    coreCode += "    for (int i = 0; i < nrow2; i++) {"
    //  Key
    key_str = ""
    for (key_col <- rightTableKeyColNames) {
      var join_right_key_col_name = stripColumnName(key_col) + "_k"
      var join_right_key_col_type = getColumnType(key_col, dfmap)
      var join_right_key_col_idx = join_right_table_col.indexOf(key_col)
      join_right_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt64(i, " + join_right_key_col_idx + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
          } else if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
          } else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
            coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_right_key_col_name + ", "
    }

    coreCode += "        auto it = ht1.find(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "}" + ");"
    coreCode += "        if (it != ht1.end()) {"
    for (right_payload <- rightTablePayloadColNames) {
      var right_payload_name = stripColumnName(right_payload)
      var right_payload_type = getColumnType(right_payload, dfmap)
      var right_payload_input_index = join_right_table_col.indexOf(right_payload)
      var right_payload_index = thisNode._outputCols.indexOf(right_payload)
      right_payload_type match {
        case "IntegerType" =>
          coreCode += "            int32_t " + right_payload_name + " = " + tbl_in_2 + ".getInt32(i, " + right_payload_input_index + ");"
        case "LongType" =>
          coreCode += "            int64_t " + right_payload_name + " = " + tbl_in_2 + ".getInt64(i, " + right_payload_input_index + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "            int32_t " + right_payload_name + " = " + tbl_in_2 + ".getInt32(i, " + right_payload_input_index + ");"
          } else if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(right_payload) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + " = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(i, " + right_payload_input_index + ");"
            }
            else {
              var rowIDNum = tbl_in_2 + ".getInt32(i, " + right_payload_input_index + ")"
              coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + " = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
            }
          } else {
            coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + " = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(i, " + right_payload_input_index + ");"
          }
        case _ =>
          coreCode += "            // Unsupported join key type"
      }
    }
    var join_filter_pairs = getJoinFilterTerms(thisNode.joining_expression(0), false)
    if (join_filter_pairs.length > 0) { //filtering is need
      var filter_expr = getJoinFilterExpression(joining_expression(0))
      coreCode += "            if " + filter_expr + " {"
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "                " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + ");"
              }
            case _ =>
              coreCode += "                // Unsupported join key type"
          }
        }
      }
      for (right_payload <- rightTablePayloadColNames) {
        var right_payload_name = stripColumnName(right_payload)
        var right_payload_type = getColumnType(right_payload, dfmap)
        var right_payload_input_index = join_right_table_col.indexOf(right_payload)
        var right_payload_index = thisNode._outputCols.indexOf(right_payload)
        if (right_payload_index != -1) {
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "                " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
              } else {
                coreCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + ");"
              }
            case _ =>
              coreCode += "                // Unsupported join key type"
          }
        }
      }
      coreCode += "                r++;"
      coreCode += "            }"
    }
    else { //no filtering - standard write out output columns
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
        }
      }
      for (right_payload <- rightTablePayloadColNames) {
        var right_payload_name = stripColumnName(right_payload)
        var right_payload_type = getColumnType(right_payload, dfmap)
        var right_payload_input_index = join_right_table_col.indexOf(right_payload)
        var right_payload_index = thisNode._outputCols.indexOf(right_payload)
        if (right_payload_index != -1) {
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
        }
      }
      coreCode += "            r++;"
    }
    coreCode += "        }"
    coreCode += "    }"
    coreCode += "    " + tbl_out_1 + ".setNumRow(r);"
  }

  def genInnerJoin_core(thisNode: SQL2FPGA_QPlan, tableOrder: String, dfmap: Map[String, DataFrame],
                        leftTableKeyColNames: ListBuffer[String], rightTableKeyColNames: ListBuffer[String],
                        leftTablePayloadColNames: ListBuffer[String], rightTablePayloadColNames: ListBuffer[String],
                        joinKeyTypeName: String, joinPayloadTypeName: String, sf: Int): ListBuffer[String] = {
    var coreCode = new ListBuffer[String]()

    // Default - leftMajor
    var tbl_in_1 = thisNode._children.head.fpgaOutputTableName
    var tbl_in_2 = thisNode._children.last.fpgaOutputTableName
    var tbl_out_1 = thisNode._fpgaOutputTableName
    var join_left_table_col = thisNode._children.head.outputCols
    var join_right_table_col = thisNode._children.last.outputCols
    var left_child = thisNode._children.head
    var right_child = thisNode._children.last

    tableOrder match {
      case "leftMajor" =>
        tbl_in_1 = thisNode._children.head.fpgaOutputTableName
        tbl_in_2 = thisNode._children.last.fpgaOutputTableName
        join_left_table_col = thisNode._children.head.outputCols
        join_right_table_col = thisNode._children.last.outputCols
        left_child = thisNode._children.head
        right_child = thisNode._children.last
      case "rightMajor" =>
        tbl_in_1 = thisNode._children.last.fpgaOutputTableName
        tbl_in_2 = thisNode._children.head.fpgaOutputTableName
        join_left_table_col = thisNode._children.last.outputCols
        join_right_table_col = thisNode._children.head.outputCols
        left_child = thisNode._children.last
        right_child = thisNode._children.head
      case _ =>
    }

    coreCode += "    std::unordered_multimap<" + joinKeyTypeName + ", " + joinPayloadTypeName + "> ht1;"
    // Enumerate input table
    var left_tbl_partition_suffix = ""
    if (sf == 30 && left_child._cpuORfpga == 1 && left_child._nodeType != "SerializeFromObject") {
      coreCode += "for (int p_idx = 0; p_idx < hpTimes; p_idx++) {"
      left_tbl_partition_suffix = "[p_idx]"
    }
    coreCode += "    int nrow1 = " + tbl_in_1 + left_tbl_partition_suffix +".getNumRow();"
    coreCode += "    for (int i = 0; i < nrow1; i++) {"
    //  Key
    var key_str = ""
    for (key_col <- leftTableKeyColNames) {
      var join_left_key_col_name = stripColumnName(key_col) + "_k"
      var join_left_key_col_type = getColumnType(key_col, dfmap)
      var join_left_key_col_idx = join_left_table_col.indexOf(key_col)
      join_left_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + left_tbl_partition_suffix + ".getInt32(i, " + join_left_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_key_col_name + " = " + tbl_in_1 + left_tbl_partition_suffix + ".getInt64(i, " + join_left_key_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + left_tbl_partition_suffix + ".getInt32(i, " + join_left_key_col_idx + ");"
          }
          else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + left_tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + left_tbl_partition_suffix + ".getInt32(i, " + join_left_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
            }
          }
          else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + left_tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
            coreCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_left_key_col_name + ", "
    }
    coreCode += "        " + joinKeyTypeName + " keyA{" + key_str.stripSuffix(", ") + "};"
    //  Payload
    var payload_str = ""
    for (payload_col <- leftTablePayloadColNames) {
      var join_left_payload_col_name = stripColumnName(payload_col)
      var join_left_payload_col_type = getColumnType(payload_col, dfmap)
      var join_left_payload_col_idx = join_left_table_col.indexOf(payload_col)
      join_left_payload_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + left_tbl_partition_suffix + ".getInt32(i, " + join_left_payload_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_left_payload_col_name + " = " + tbl_in_1 + left_tbl_partition_suffix + ".getInt64(i, " + join_left_payload_col_idx + ");"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + left_tbl_partition_suffix + ".getInt32(i, " + join_left_payload_col_idx + ");"
          }
          else if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(payload_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + left_tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_1 + left_tbl_partition_suffix + ".getInt32(i, " + join_left_payload_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
            }
          }
          else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + left_tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
            coreCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      payload_str += join_left_payload_col_name + ", "
    }
    coreCode += "        " + joinPayloadTypeName + " payloadA{" + payload_str.stripSuffix(", ") + "};"
    coreCode += "        ht1.insert(std::make_pair(keyA, payloadA));"
    coreCode += "    }"
    if (sf == 30 && left_child._cpuORfpga == 1 && left_child._nodeType != "SerializeFromObject") {
      coreCode += "}"
    }

    coreCode += "    int r = 0;"
    // Enumerate input table
    var right_tbl_partition_suffix = ""
    if (sf == 30 && right_child._cpuORfpga == 1 && right_child._nodeType != "SerializeFromObject") {
      coreCode += "for (int p_idx = 0; p_idx < hpTimes; p_idx++) {"
      right_tbl_partition_suffix = "[p_idx]"
    }
    coreCode += "    int nrow2 = " + tbl_in_2 + right_tbl_partition_suffix +".getNumRow();"
    // Enumerate output table
    coreCode += "    for (int i = 0; i < nrow2; i++) {"
    //  Key
    key_str = ""
    for (key_col <- rightTableKeyColNames) {
      var join_right_key_col_name = stripColumnName(key_col) + "_k"
      var join_right_key_col_type = getColumnType(key_col, dfmap)
      var join_right_key_col_idx = join_right_table_col.indexOf(key_col)
      join_right_key_col_type match {
        case "IntegerType" =>
          coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getInt32(i, " + join_right_key_col_idx + ");"
        case "LongType" =>
          coreCode += "        int64_t " + join_right_key_col_name + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getInt64(i, " + join_right_key_col_idx + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getInt32(i, " + join_right_key_col_idx + ");"
          }
          else if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(key_col) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_2 + right_tbl_partition_suffix + ".getInt32(i, " + join_right_key_col_idx + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
            }
          }
          else {
            coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_2 + right_tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
            coreCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
          }
        case _ =>
          coreCode += "        // Unsupported join key type"
      }
      key_str += join_right_key_col_name + ", "
    }
    coreCode += "        auto its = ht1.equal_range(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "});"
    coreCode += "        auto it = its.first;"
    coreCode += "        while (it != its.second) {"
    //  Payload
    for (left_payload <- leftTablePayloadColNames) {
      var left_payload_name = stripColumnName(left_payload)
      var left_payload_type = getColumnType(left_payload, dfmap)
      left_payload_type match {
        case "IntegerType" =>
          coreCode += "            int32_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
        case "LongType" =>
          coreCode += "            int64_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
        case "StringType" =>
          if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "            int32_t " + left_payload_name + " = (it->second)." + left_payload_name + ";"
          }
          else {
            coreCode += "            std::string " + left_payload_name + " = (it->second)." + left_payload_name + ";"
            coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1> " + left_payload_name + "_n" + "{};"
            coreCode += "            memcpy(" + left_payload_name + "_n" + ".data(), (" + left_payload_name + ").data(), (" + left_payload_name + ").length());"
          }
        case _ =>
          coreCode += "            // Unsupported join key type"
      }
    }
    for (right_payload <- rightTablePayloadColNames) {
      var right_payload_name = stripColumnName(right_payload)
      var right_payload_type = getColumnType(right_payload, dfmap)
      var right_payload_input_index = join_right_table_col.indexOf(right_payload)
      right_payload_type match {
        case "IntegerType" =>
          coreCode += "            int32_t " + right_payload_name + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getInt32(i, " + right_payload_input_index + ");"
        case "LongType" =>
          coreCode += "            int64_t " + right_payload_name + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getInt64(i, " + right_payload_input_index + ");"
        case "StringType" =>
          if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
            coreCode += "            int32_t " + right_payload_name + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getInt32(i, " + right_payload_input_index + ");"
          }
          else if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == true) {
            //find the original stringRowID table that contains this string data
            var orig_table_names = get_stringRowIDOriginalTableName(thisNode)
            var orig_table_columns = get_stringRowIDOriginalTableColumns(thisNode)
            var orig_tbl_idx = -1
            var orig_tbl_col_idx = -1
            for (orig_tbl <- orig_table_columns) {
              for (col <- orig_tbl) {
                if (columnDictionary(right_payload) == columnDictionary(col)) {
                  orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                  orig_tbl_col_idx = orig_tbl.indexOf(col)
                }
              }
            }
            //find the col index of the string data in the original stringRowID table
            if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
              coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + "_n" + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(i, " + right_payload_input_index + ");"
              coreCode += "            std::string " + right_payload_name + " = std::string(" + right_payload_name + "_n.data());"
            }
            else {
              var rowIDNum = tbl_in_2 + right_tbl_partition_suffix + ".getInt32(i, " + right_payload_input_index + ")"
              coreCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + "_n = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
              coreCode += "        std::string " + right_payload_name + " = std::string(" + right_payload_name + "_n.data());"
            }
          }
          else {
            coreCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + "_n" + " = " + tbl_in_2 + right_tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(i, " + right_payload_input_index + ");"
            coreCode += "            std::string " + right_payload_name + " = std::string(" + right_payload_name + "_n.data());"
          }
        case _ =>
          coreCode += "            // Unsupported join key type"
      }
    }
    var join_filter_pairs = getJoinFilterTerms(thisNode.joining_expression(0), false)
    if (join_filter_pairs.length > 0) { //filtering is need
      var filter_expr = getJoinFilterExpression(thisNode.joining_expression(0))
      coreCode += "            if " + filter_expr + " {"
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "                " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "                // Unsupported join key type"
          }
        }
      }
      for (right_payload <- rightTablePayloadColNames) {
        var right_payload_name = stripColumnName(right_payload)
        var right_payload_type = getColumnType(right_payload, dfmap)
        var right_payload_input_index = join_right_table_col.indexOf(right_payload)
        var right_payload_index = thisNode._outputCols.indexOf(right_payload)
        if (right_payload_index != -1) {
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "                " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
              } else {
                coreCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "                // Unsupported join key type"
          }
        }
      }
      coreCode += "                r++;"
      coreCode += "            }"
      coreCode += "            it++;"
    }
    else { //no filtering - standard write out output columns
      for (left_payload <- leftTablePayloadColNames) {
        var left_payload_name = stripColumnName(left_payload)
        var left_payload_type = getColumnType(left_payload, dfmap)
        var left_payload_index = thisNode._outputCols.indexOf(left_payload)
        if (left_payload_index != -1) {
          left_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
            case "StringType" =>
              if (left_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
        }
      }
      for (right_payload <- rightTablePayloadColNames) {
        var right_payload_name = stripColumnName(right_payload)
        var right_payload_type = getColumnType(right_payload, dfmap)
        var right_payload_input_index = join_right_table_col.indexOf(right_payload)
        var right_payload_index = thisNode._outputCols.indexOf(right_payload)
        if (right_payload_index != -1) {
          right_payload_type match {
            case "IntegerType" =>
              coreCode += "            " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "LongType" =>
              coreCode += "            " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
            case "StringType" =>
              if (right_child._stringRowIDSubstitution == true && thisNode._stringRowIDBackSubstitution == false) {
                coreCode += "            " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
              } else {
                coreCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + "_n" + ");"
              }
            case _ =>
              coreCode += "            // Unsupported join key type"
          }
        }
      }
      coreCode += "            it++;"
      coreCode += "            r++;"
    }
    coreCode += "        }"
    coreCode += "    }"
    if (sf == 30 && right_child._cpuORfpga == 1 && right_child._nodeType != "SerializeFromObject") {
      coreCode += "}"
    }
    coreCode += "    " + tbl_out_1 + ".setNumRow(r);"

    return coreCode
  }

  def get_stringRowIDOriginalTableName(this_node: SQL2FPGA_QPlan): ListBuffer[String] = {
    var results = new ListBuffer[String]

    //prev version - restricted to direct data source to operator relation
    // if (this_node._nodeType == "SerializeFromObject" && this_node._stringRowIDSubstitution == true) {
    //   results += this_node._fpgaOutputTableName_stringRowIDSubstitute
    //   return results
    // }
    var noSubstitutionChildren = true
    for (ch <- this_node._children) {
      if (ch._stringRowIDSubstitution == true) {
        noSubstitutionChildren = false
      }
    }
    if (noSubstitutionChildren && this_node._stringRowIDSubstitution == true) {
      if (this_node._fpgaOutputTableName_stringRowIDSubstitute != "NULL") {
        results += this_node._fpgaOutputTableName_stringRowIDSubstitute
      }
      else {
        results += this_node._children.head._fpgaOutputTableName
      }

      return results
    }

    for (ch <- this_node._children) {
      var newTableList = get_stringRowIDOriginalTableName(ch)
      for (tbl <- newTableList) {
        if (!results.contains(tbl)) {
          results += tbl
        }
      }
    }

    return results
  }

  def get_stringRowIDOriginalTableColumns(this_node: SQL2FPGA_QPlan): ListBuffer[ListBuffer[String]] = {
    var results = new ListBuffer[ListBuffer[String]]

    //prev version - restricted to direct data source to operator relation
    // if (this_node._nodeType == "SerializeFromObject" && this_node._stringRowIDSubstitution == true) {
    //   results += this_node._outputCols
    //   return results
    // }
    var noSubstitutionChildren = true
    for (ch <- this_node._children) {
      if (ch._stringRowIDSubstitution == true) {
        noSubstitutionChildren = false
      }
    }
    if (noSubstitutionChildren && this_node._stringRowIDSubstitution == true) {
      results += this_node._outputCols
      return results
    }

    for (ch <- this_node._children) {
      var tmp_storage = get_stringRowIDOriginalTableColumns(ch)
      // below is to prevent adding duplicated table column lists
      for (list <- tmp_storage) {
        if (!results.contains(list)) {
          results += list
        }
      }
    }

    return results
  }

  def printPlan: Unit = {
    print_indent(_treeDepth)
    println("Node Type: " + _nodeType)
    if (_outputCols.nonEmpty) {
      print_indent(_treeDepth)
      println("Output Columns: " + _outputCols.toString())
    }
    if (_operation.nonEmpty) {
      print_indent(_treeDepth)
      println("Operations Details: " + _operation.toString())
      if (_aggregate_operation.nonEmpty){
        print_indent(_treeDepth)
        println("\tAggregation: " + _aggregate_operation.toString())
      }
      if (_groupBy_operation.nonEmpty){
        print_indent(_treeDepth)
        println("\tGroupBy: " + _groupBy_operation.toString())
      }
    }
    if (_children.isEmpty) {
      print_indent(_treeDepth)
      println("Input Columns: " + _inputCols.toString())
    } else {
      print_indent(_treeDepth)
      print("Input Columns: ")
      for (ch <- _children) {
        print(ch.outputCols.toString() + " ")
      }
      print("\n")
    }
    for (ch <- _children) {
      ch.printPlan
    }
  }

  def printPlan_InOrder(dfmap: Map[String, DataFrame]): Unit = {
    for (ch <- _children) {
      ch.printPlan_InOrder(dfmap)
    }
    //      if (nodeType != "SerializeFromObject") {
    if (nodeType != "NULL") {
      print(this.toString.split("\\$").last + " - ")
      print(_treeDepth)
      print_indent(_treeDepth)
      if (_fpgaOverlayType == 0) {
        println("Node Type: " + _nodeType + " | CPU/FPGA: " + _cpuORfpga + " | OverlayID: " + _fpgaOverlayID + " | OverlayType: " + _fpgaOverlayType + " | OverlayDepth: " + _fpgaJoinOverlayPipelineDepth + " | StringRowIDSubstitution: " + _stringRowIDSubstitution + " | StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution)
      } else if (_fpgaOverlayType == 1) {
        println("Node Type: " + _nodeType + " | CPU/FPGA: " + _cpuORfpga + " | OverlayID: " + _fpgaOverlayID + " | OverlayType: " + _fpgaOverlayType + " | OverlayDepth: " + _fpgaAggrOverlayPipelineDepth + " | StringRowIDSubstitution: " + _stringRowIDSubstitution + " | StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution)
      }
      if (_children.isEmpty) {
        print_indent(_treeDepth)
        print("Input Columns: " + _inputCols.toString() + " ")
        var col_type_list = "["
        for (col <- _inputCols) {
          col_type_list += getColumnType(col, dfmap) + ", "
        }
        col_type_list = col_type_list.stripSuffix(", ")
        col_type_list += "]"
        print(col_type_list + "\n")
      } else {
        print_indent(_treeDepth)
        print("Input Columns: ")
        var first_child = true
        for (ch <- _children) {
          if (first_child) {
            first_child = false
          }
          else {
            print_indent(_treeDepth)
            print("               ")
          }
          print(ch.outputCols.toString() + " ")
          var col_type_list = "["
          for (col <- ch._outputCols) {
            col_type_list += getColumnType(col, dfmap) + ", "
          }
          col_type_list = col_type_list.stripSuffix(", ")
          col_type_list += "]"
          print(col_type_list + "\n")
        }
      }
      if (_operation.nonEmpty) {
        print_indent(_treeDepth)
        println("Operations Details: " + _operation.toString())
        if (_aggregate_operation.nonEmpty){
          print_indent(_treeDepth)
          println("\tAggregation: " + _aggregate_operation.toString())
        }
        if (_groupBy_operation.nonEmpty){
          print_indent(_treeDepth)
          println("\tGroupBy: " + _groupBy_operation.toString())
        }
      }
      if (_outputCols.nonEmpty) {
        print_indent(_treeDepth)
        println("Output Columns: " + _outputCols.toString())
      }
    }
  }

  def operatorIsolation(): Int = {
    var num_overlay_invokes = 0
    var q_node = Queue[SQL2FPGA_QPlan](this)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        if (this_node._cpuORfpga == 1 && this_node._nodeType != "SerializeFromObject") {
          num_overlay_invokes += 1
        }
        for (ch <- this_node._children) {
          if (ch._parent.isEmpty)
            ch._parent += this_node
          tmp_q.enqueue(ch)
        }
      }
      q_node = tmp_q
    }
    return num_overlay_invokes
  }

  def operatorFusion_allocation(overlayID: Int): Int = {
    if (this._children.isEmpty) {
      return 0
    }

    var num_overlay_invokes = 0
    var fpgaOverlayCompatible = (this._cpuORfpga == 1)
    if (fpgaOverlayCompatible) {
      this._fpgaOverlayType = 0 //default as gqe-join
      this._nodeType match {
        case "Filter" =>
          _fpgaJoinOverlayPipelineDepth = 1
          _fpgaAggrOverlayPipelineDepth = 2
        case "JOIN_INNER" =>
          _fpgaJoinOverlayPipelineDepth = 2
          _fpgaAggrOverlayPipelineDepth = -1
        case "JOIN_LEFTANTI" =>
          _fpgaJoinOverlayPipelineDepth = 2 //disable fusion between filter and antijoin
          _fpgaAggrOverlayPipelineDepth = -1
        case "JOIN_LEFTSEMI" =>
          _fpgaJoinOverlayPipelineDepth = 2 //disable fusion between filter and semijoin
          _fpgaAggrOverlayPipelineDepth = -1
        case "JOIN_LEFTOUTER" =>
          _fpgaJoinOverlayPipelineDepth = 2
          _fpgaAggrOverlayPipelineDepth = -1
        case "Aggregate" =>
          if (this._groupBy_operation.nonEmpty) {
            _fpgaOverlayType = 1 //switch to gqe-aggr
            _fpgaJoinOverlayPipelineDepth = -1
            _fpgaAggrOverlayPipelineDepth = 3
            //Optimization: to allow fusion between filter operator and groupby operator (w/ evaluation)
            //              if (this._aggregate_expression.nonEmpty && !isPureAggrOperation(this._aggregate_expression)) {
            //                _fpgaAggrOverlayPipelineDepth = 1
            //              }
          }
          else {
            _fpgaJoinOverlayPipelineDepth = 3
            _fpgaAggrOverlayPipelineDepth = -1
            if (isPureAggrOperation(this._aggregate_expression)) {
              _fpgaJoinOverlayPipelineDepth = 4
              _fpgaAggrOverlayPipelineDepth = 4
            }
            if (isPureEvalOperation(this._aggregate_expression)) {
              _fpgaAggrOverlayPipelineDepth = 1
            }
          }
        case "Project" =>
          // fused
          //            _fpgaJoinOverlayPipelineDepth = 3
          //            _fpgaAggrOverlayPipelineDepth = 1
          // non-fused
          _fpgaJoinOverlayPipelineDepth = 0
          _fpgaAggrOverlayPipelineDepth = 0
        // case "SerializeFromObject" =>
        case _ =>
          _fpgaJoinOverlayPipelineDepth = 0
          _fpgaAggrOverlayPipelineDepth = 0
      }
    }

    var isRootNode = this._parent.isEmpty
    if (isRootNode) {
      if (fpgaOverlayCompatible) num_overlay_invokes = 1 else num_overlay_invokes = 0
      for (ch <- this._children) {
        if (fpgaOverlayCompatible) {
          num_overlay_invokes += ch.operatorFusion_allocation(overlayID + 1)
        } else {
          num_overlay_invokes += ch.operatorFusion_allocation(overlayID + 0)
        }
      }
      if (fpgaOverlayCompatible) this._fpgaOverlayID = overlayID + 1
    }
    else {
      var isCpuParentNode = (this._parent.head._cpuORfpga == 0)
      if (isCpuParentNode) {
        if (fpgaOverlayCompatible) num_overlay_invokes = 1 else num_overlay_invokes = 0
        for (ch <- this._children) {
          if (fpgaOverlayCompatible) {
            num_overlay_invokes += ch.operatorFusion_allocation(overlayID + 1)
          } else {
            num_overlay_invokes += ch.operatorFusion_allocation(overlayID + 0)
          }
        }
        if (fpgaOverlayCompatible) this._fpgaOverlayID = overlayID + 1
      }
      else {
        if (fpgaOverlayCompatible) {
          var fusible_increment = overlayID + 0
          if (this._fpgaOverlayType == 1) { // gqe-aggr
            if (this._parent.head._fpgaAggrOverlayPipelineDepth > this._fpgaAggrOverlayPipelineDepth) { //fusible
              this._parent.head._fpgaOverlayType = 1
              num_overlay_invokes = 0
              fusible_increment = overlayID + 0
            }
            else {
              num_overlay_invokes = 1
              fusible_increment = overlayID + 1
            }
            for (ch <- this._children) {
              num_overlay_invokes += ch.operatorFusion_allocation(fusible_increment)
            }
            this._fpgaOverlayID = fusible_increment
          }
          else if (this._fpgaOverlayType == 0) { // gqe-join
            var num_overlay_invokes_join = -1
            var num_overlay_invokes_aggr = -1
            if (this._parent.head._fpgaJoinOverlayPipelineDepth > this._fpgaJoinOverlayPipelineDepth &&
              this._parent.head._fpgaJoinOverlayPipelineDepth != -1 && this._fpgaJoinOverlayPipelineDepth != -1) { //fusible
              this._fpgaOverlayType == 0
              num_overlay_invokes_join = 0
              for (ch <- this._children) {
                num_overlay_invokes_join += ch.operatorFusion_allocation(fusible_increment)
              }
            }
            if (this._parent.head._fpgaAggrOverlayPipelineDepth > this._fpgaAggrOverlayPipelineDepth &&
              this._parent.head._fpgaAggrOverlayPipelineDepth != -1 && this._fpgaAggrOverlayPipelineDepth != -1) { //fusible
              this._fpgaOverlayType == 1
              num_overlay_invokes_aggr = 0
              for (ch <- this._children) {
                num_overlay_invokes_aggr += ch.operatorFusion_allocation(fusible_increment)
              }
            }
            if (num_overlay_invokes_join == -1 && num_overlay_invokes_aggr == -1) { //not fusible
              num_overlay_invokes = 1
              fusible_increment = overlayID + 1
              for (ch <- this._children) {
                num_overlay_invokes += ch.operatorFusion_allocation(fusible_increment)
              }
              this._fpgaOverlayID = fusible_increment
            }
            else { //fusible
              if (num_overlay_invokes_aggr == -1) { // gqe-join fusible
                num_overlay_invokes = num_overlay_invokes_join
              } else if (num_overlay_invokes_join == -1) { // gqe-aggr fusible
                num_overlay_invokes = num_overlay_invokes_aggr
              } else { // fusible with gqe-join and gqe-aggr
                if (num_overlay_invokes_join > num_overlay_invokes_aggr) { // gqe-aggr better
                  num_overlay_invokes = num_overlay_invokes_aggr
                } else {
                  this._fpgaOverlayType == 0
                  num_overlay_invokes = num_overlay_invokes_join
                }
              }
              this._fpgaOverlayID = fusible_increment
            }
          }
          else { // not supported
            println("ERROR")
            return -1
          }
        }
        else {
          num_overlay_invokes = 0
          for (ch <- this._children) {
            num_overlay_invokes += ch.operatorFusion_allocation(overlayID + 0)
          }
        }
      }
    }

    return num_overlay_invokes
  }

  def operatorFusion_binding(overlayID: Int): ListBuffer[SQL2FPGA_QPlan] = {
    var bindedOverlay = new ListBuffer[SQL2FPGA_QPlan]()
    var tmp_children_deep_copy = _children.clone()
    for (ch <- tmp_children_deep_copy) {
      var temp = ch.operatorFusion_binding(this._fpgaOverlayID)
      if (temp.nonEmpty) {
        if (_children.length == 1) { // non-join typed child
          _bindedOverlayInstances = temp ++ _bindedOverlayInstances
        }
        else if (_children.length == 2) { // join typed child
          if (_children.indexOf(ch) == 0) { // left child
            _bindedOverlayInstances_left = temp ++ _bindedOverlayInstances_left
          }
          else if (_children.indexOf(ch) == 1) { // right child
            _bindedOverlayInstances_right = temp ++ _bindedOverlayInstances_right
          }
        }
        else { // Error Case
        }
        var removed_ch_idx = _children.indexOf(ch)
        _children.remove(removed_ch_idx)
        for (sub_ch <- ch._children) {
          _children.insert(removed_ch_idx, sub_ch)
          removed_ch_idx += 1
        }
      }
    }
    if (this._fpgaOverlayID == overlayID && overlayID != -1) {
      bindedOverlay += this
    }
    return bindedOverlay
  }

  def optimizeFPGAOperator(dfmap: Map[String, DataFrame]): (Int, Int) = {
    var num_overlay_naive = operatorIsolation()
    var num_overlay_fused = operatorFusion_allocation(0)
    this.printPlan_InOrder(dfmap)
    print("\n")

    operatorFusion_binding(_fpgaOverlayID)
    println("num_overlays (naive plan): " + num_overlay_naive)
    println("num_overlays (fused plan): " + num_overlay_fused)
    this.printPlan_InOrder(dfmap)
    print("\n")
    return (num_overlay_naive, num_overlay_fused)
  }

  def getOverlayCallCount(): Unit ={
    if (this._cpuORfpga == 1 && this._fpgaOverlayType == 0 && this._children.nonEmpty) {
      this._fpgaJoinOverlayCallCount += 1
    }
    if (this._cpuORfpga == 1 && this._fpgaOverlayType == 1 && this._children.nonEmpty) {
      this._fpgaAggrOverlayCallCount += 1
    }
    for (ch <- this._children) {
      ch.getOverlayCallCount()
      this._fpgaJoinOverlayCallCount += ch._fpgaJoinOverlayCallCount
      this._fpgaAggrOverlayCallCount += ch._fpgaAggrOverlayCallCount
    }
  }

  def addChildrenParentConnections(dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit = {
    var num_overlay_invokes = 0
    var q_node = Queue[SQL2FPGA_QPlan](this)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        for (ch <- this_node._children) {
          if (ch._parent.isEmpty)
            ch._parent += this_node
          tmp_q.enqueue(ch)
        }
      }
      q_node = tmp_q
    }
  }

  def stringRowIDSubstitution_tagging(dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit = {
    var q_node = Queue[SQL2FPGA_QPlan](this)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      // Breath-First Traversal
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        println(this_node._nodeType)

        if (this_node._operation.isEmpty) {
          if (this_node._nodeType == "SerializeFromObject") {
            var inputColsContainStringType = false
            for (i_col <- this_node._outputCols) {
              if (getColumnType(i_col, dfmap) == "StringType") {
                inputColsContainStringType = true
              }
            }
            if (inputColsContainStringType) {
              this_node._stringRowIDSubstitution = true
            }
          }
          else {
            println("************************* INVALID NODETYPE ****************************")
            println(this_node._nodeType)
            println("************************* INVALID NODETYPE ****************************")
          }
        }
        else {
          var inputColsContainStringType = false
          for (ch <- this_node._children) {
            for (o_col <- ch._outputCols) {
              if (getColumnType(o_col, dfmap) == "StringType") {
                inputColsContainStringType = true
              }
            }
          }
          var operationContainStringType = false
          if (this_node._nodeType == "JOIN_INNER" ||
            this_node._nodeType == "JOIN_LEFTANTI" ||
            this_node._nodeType == "JOIN_LEFTSEMI" ||
            this_node._nodeType == "JOIN_LEFTOUTER") {
            println(this_node._joining_expression(0).references.toString)
            for (join_expr <- this_node._joining_expression) {
              for (col <- join_expr.references) {
                if (getColumnType(col.toString, dfmap) == "StringType") {
                  operationContainStringType = true
                }
              }
            }
          }
          else if (this_node._nodeType == "Filter") {
            for (col <- this_node._filtering_expression.references) {
              if (getColumnType(col.toString, dfmap) == "StringType") {
                operationContainStringType = true
              }
            }
          }
          else if (this_node._nodeType == "Aggregate" || this_node._nodeType == "Project") {
            for (aggr_expr <- this_node._aggregate_expression) {
              for (col <- aggr_expr.references) {
                if (getColumnType(col.toString, dfmap) == "StringType") {
                  operationContainStringType = true
                }
              }
            }

            for (groupBy_expr <- this_node._groupBy_expression) {
              if (groupBy_expr.dataType.toString == "StringType") {
                operationContainStringType = true
              }
            }
          }
          else if (this_node._nodeType == "Sort") {
            //FIXME: always not avaliable for joining
            operationContainStringType = true
          }
          else {
            println("************************* INVALID NODETYPE ****************************")
            println(this_node._nodeType)
            println("************************* INVALID NODETYPE ****************************")
          }

          if (inputColsContainStringType == true && operationContainStringType == false) {
            this_node._stringRowIDSubstitution = true
          }
        }

        for (ch <- this_node._children) {
          if (ch._parent.isEmpty)
            ch._parent += this_node
          tmp_q.enqueue(ch)
        }
      }
      println("")
      q_node = tmp_q
    }
  }

  def stringRowIDSubstitution_pruning(dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit ={
    // end case
    if (this._nodeType == "SerializeFromObject") {
      if (this._parent.head._stringRowIDSubstitution == false) {
        this._stringRowIDSubstitution = false
        this._fpgaOutputTableName_stringRowIDSubstitute = "NULL"
      }
      return
    }
    // recursive case
    for (ch <- this._children) {
      ch.stringRowIDSubstitution_pruning(dfmap, pure_sw_mode)
    }
    for (ch <- this._children) {
      // string back-substitution
      if (this._stringRowIDSubstitution == false && ch._stringRowIDSubstitution == true) {
        this._stringRowIDBackSubstitution = true
      }
    }
    // invalid substitution removal
    //      if (this._stringRowIDSubstitution == true) {
    //        //FIXME: this restricts string-rowid substitution to direct table source to operator relation
    //        var allChildrenInvalid = true
    //        for (ch <- this._children) {
    //          if (ch._stringRowIDSubstitution == true) {
    //            allChildrenInvalid = false
    //          }
    //        }
    //        if (allChildrenInvalid) {
    //          this._stringRowIDSubstitution = false
    //        }
    //      }

  }

  def stringRowIDSubstitution_rescan(dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit ={
    // end case
    if (this._nodeType == "Filter") {
      if (this._parent.nonEmpty) {
        if (this._parent.head._stringRowIDSubstitution == true && this._stringRowIDSubstitution == false) {
          this._stringRowIDSubstitution = true
          _fpgaInputTableName_stringRowIDSubstitute = this._children.head._fpgaOutputTableName
          _fpgaOutputTableName_stringRowIDSubstitute = _fpgaInputTableName_stringRowIDSubstitute
        }
      }
    }
    // recursive case
    for (ch <- this._children) {
      ch.stringRowIDSubstitution_rescan(dfmap, pure_sw_mode)
    }
  }

  def applyStringDataTypeOptTransform(qParser: SQL2FPGA_QParser, qConfig: SQL2FPGA_QConfig, dfmap: Map[String, DataFrame]): Unit ={
    qParser.qPlan.stringRowIDSubstitution_tagging(dfmap, qConfig.pure_sw_mode)
    if (DEBUG_STRINGDATATYPE_OPT == true) {
      println(" AFTER stringRowIDSubstitution_tagging()")
      qParser.qPlan.printPlan_InOrder(dfmap)
    }
    qParser.qPlan.stringRowIDSubstitution_pruning(dfmap, qConfig.pure_sw_mode)
    if (DEBUG_STRINGDATATYPE_OPT == true) {
      println(" AFTER stringRowIDSubstitution_pruning()")
      qParser.qPlan.printPlan_InOrder(dfmap)
    }
    qParser.qPlan.stringRowIDSubstitution_rescan(dfmap, qConfig.pure_sw_mode)
    if (DEBUG_STRINGDATATYPE_OPT == true) {
      println(" AFTER stringRowIDSubstitution_rescan()")
      qParser.qPlan.printPlan_InOrder(dfmap)
    }
  }

  def getMaxCascadeJoinLength(currentMaxLength: Int): Int = {
    // end case
    if (this._children.isEmpty)
      return 0

    // recursive case
    var result_length = 0
    if (this._nodeType == "JOIN_INNER")
      result_length = currentMaxLength + 1

    var nextMaxLength = 0
    for (ch <- this._children) {
      var temp_length = ch.getMaxCascadeJoinLength(result_length)
      if (temp_length > nextMaxLength) {
        nextMaxLength = temp_length
      }
    }
    if (nextMaxLength > currentMaxLength) {
      return nextMaxLength
    } else {
      return currentMaxLength
    }
  }

  def getMaxCascadeJoinChain(thisNode: SQL2FPGA_QPlan, currentMaxLength: Int): (Int, ListBuffer[Expression], ListBuffer[String], ListBuffer[SQL2FPGA_QPlan]) = {
    var result_expr = new ListBuffer[Expression]
    var result_filter = new ListBuffer[String]
    var result_node = new ListBuffer[SQL2FPGA_QPlan]

    // end case
    if (thisNode._children.isEmpty)
      return (0, result_expr, result_filter, result_node)

    // recursive case
    var result_length = 0
    if (thisNode._nodeType == "JOIN_INNER") {
      // var join_filter_pairs = getJoinFilterTerms(thisNode._joining_expression(0), false)
      var join_filter_pairs = new ListBuffer[String]
      if (join_filter_pairs.length == 0) { // supports join without filtering
        result_length = currentMaxLength + 1
        result_expr ++= thisNode._joining_expression
        if (thisNode._children.head._nodeType == "Filter" || thisNode._children.last._nodeType == "Filter") {
          result_filter += "true"
        } else {
          result_filter += "false"
        }
        result_node += thisNode
      }
    }

    var nextMaxLength = 0
    for (ch <- thisNode._children) {
      var (temp_length, temp_expr, temp_filter, temp_node) = getMaxCascadeJoinChain(ch, result_length)
      if (temp_length > nextMaxLength) {
        nextMaxLength = temp_length
        result_expr ++= temp_expr
        result_filter ++= temp_filter
        result_node ++= temp_node
      }
    }

    if (nextMaxLength > currentMaxLength) {
      return (nextMaxLength, result_expr, result_filter, result_node)
    } else {
      return (currentMaxLength, result_expr, result_filter, result_node)
    }
  }

  def getAllCascadeJoinChains(thisNode: SQL2FPGA_QPlan, currentChain: ListBuffer[SQL2FPGA_QPlan]): ListBuffer[ListBuffer[SQL2FPGA_QPlan]] = {
    var result = new ListBuffer[ListBuffer[SQL2FPGA_QPlan]]
    var newList = new ListBuffer[SQL2FPGA_QPlan]

    if (thisNode._nodeType == "JOIN_INNER") {
      if (thisNode.getJoinFilterTerms(thisNode._joining_expression(0), false).isEmpty) {
        newList ++= currentChain
        newList += thisNode
      }
    } else {
      if (currentChain.length != 0) {
        result += currentChain
      }
    }

    for (ch <- thisNode._children) {
      result ++= getAllCascadeJoinChains(ch, newList)
    }

    return result
  }

  def getParentsOtherChild(parentNode :SQL2FPGA_QPlan, datum :SQL2FPGA_QPlan): SQL2FPGA_QPlan = {
    var result_child = new SQL2FPGA_QPlan
    for (ch <- parentNode._children) {
      if (ch != datum) {
        return ch
      }
    }
    return result_child // should never reach here
  }

  def swapTopandBottom(joinNodeList: ListBuffer[SQL2FPGA_QPlan], topIdx: Int, bottomIdx: Int): Unit ={
    // Keep track of the orig output cols
    var orig_output = joinNodeList(topIdx)._outputCols.clone()

    // children swap - FIXME: need to cover corner cases (e.g., bottom node is a direct child of the top node)
    var top_joinChild_idx = joinNodeList(topIdx)._children.indexOf(joinNodeList(topIdx+1))
    var btm_joinChild_idx = joinNodeList(bottomIdx)._children.indexOf(joinNodeList(bottomIdx+1))
    joinNodeList(topIdx)._children(top_joinChild_idx) = joinNodeList(bottomIdx+1)
    joinNodeList(bottomIdx)._children(btm_joinChild_idx) = joinNodeList(topIdx+1)

    joinNodeList(topIdx)._children(top_joinChild_idx)._parent.clear()
    joinNodeList(topIdx)._children(top_joinChild_idx)._parent += joinNodeList(topIdx)
    joinNodeList(bottomIdx)._children(btm_joinChild_idx)._parent.clear()
    joinNodeList(bottomIdx)._children(btm_joinChild_idx)._parent += joinNodeList(bottomIdx)

    // parent swap
    var top_thisNodeOfParents_idx = joinNodeList(topIdx)._parent.head._children.indexOf(joinNodeList(topIdx))
    var btm_thisNodeOfParents_idx = joinNodeList(bottomIdx)._parent.head._children.indexOf(joinNodeList(bottomIdx))
    joinNodeList(topIdx)._parent.head._children(top_thisNodeOfParents_idx) = joinNodeList(bottomIdx)
    joinNodeList(bottomIdx)._parent.head._children(btm_thisNodeOfParents_idx) = joinNodeList(topIdx)

    var tmp_parent_node = joinNodeList(topIdx)._parent.clone()
    joinNodeList(topIdx)._parent = joinNodeList(bottomIdx)._parent
    joinNodeList(bottomIdx)._parent = tmp_parent_node

    // Depth swap
    var tmp_depth = joinNodeList(topIdx)._treeDepth
    joinNodeList(topIdx)._treeDepth = joinNodeList(bottomIdx)._treeDepth
    joinNodeList(bottomIdx)._treeDepth = tmp_depth

    // Recursively update parents' output column list
    var node_iter = joinNodeList(topIdx)
    var remove_join_key_list = new ListBuffer[String]
    var join_key_list = node_iter.getJoinKeyTerms(node_iter._joining_expression(0), false)
    for (key_pair <- join_key_list) {
      var key_pair_formatted = key_pair.split(" ").to(ListBuffer)
      key_pair_formatted.remove(1, 1) // "=" or "!=" in key pair term
      for (key <- key_pair_formatted) {
        if (!node_iter._outputCols.contains(key)) {
          remove_join_key_list += key
        }
      }
    }
    while (node_iter.nodeType == "JOIN_INNER") { //head of cascaded join
      var updated_ouput_list = new ListBuffer[String]
      for (o_col <- node_iter._outputCols) {
        var otherChild = getParentsOtherChild(node_iter._parent(0), node_iter)
        var notFoundInOtherChildOutputList = !otherChild._outputCols.contains(o_col)
        if (!remove_join_key_list.contains(o_col) && notFoundInOtherChildOutputList)
          updated_ouput_list += o_col
      }
      //add back any missing cols from the orig output cols
      for (orig_o_col <- orig_output) {
        var otherChild = getParentsOtherChild(node_iter._parent(0), node_iter)
        var notFoundInOutputList = !updated_ouput_list.contains(orig_o_col)
        var notFoundInOtherChildOutputList = !otherChild._outputCols.contains(orig_o_col)
        if (notFoundInOutputList && notFoundInOtherChildOutputList) {
          updated_ouput_list += orig_o_col
        }
      }
      node_iter._outputCols = updated_ouput_list
      node_iter = node_iter._parent(0) // iterate to next parent
    }
  }

  def getJoinChild(datum: SQL2FPGA_QPlan): SQL2FPGA_QPlan ={
    var result_child = new SQL2FPGA_QPlan
    for (ch <- datum._children) {
      if (ch._nodeType.contains("JOIN")) {
        return ch
      }
    }
    return result_child // should never reach here
  }

  def isFromInputColumnSources(datum: SQL2FPGA_QPlan, col_name: String): Boolean ={
    var result_bool = false
    for (ch <- datum._children) {
      if (ch._outputCols.contains(col_name)) {
        result_bool = true
      }
    }
    return result_bool
  }

  def InsertTopBeforeBottom(joinNodeList: ListBuffer[SQL2FPGA_QPlan], topIdx: Int, bottomIdx: Int): Unit ={

    // top_node.join_child._parent = top_node._parent
    var top_node_join_child = getJoinChild(joinNodeList(topIdx))
    top_node_join_child._parent = joinNodeList(topIdx)._parent.clone()

    // top_node._parent.join_child = top_node.join_child
    var tmp_idx = joinNodeList(topIdx)._parent(0)._children.indexOf(getJoinChild(joinNodeList(topIdx)._parent(0)))
    joinNodeList(topIdx)._parent(0)._children(tmp_idx) = top_node_join_child

    // top_node._parent = bottom_node
    var newParentList = new ListBuffer[SQL2FPGA_QPlan]
    newParentList += joinNodeList(bottomIdx)
    joinNodeList(topIdx)._parent = newParentList

    // top_node.join_child = bottom_node.join_child
    var bottom_node_join_child = getJoinChild(joinNodeList(bottomIdx))
    var top_node_join_child_idx = joinNodeList(topIdx)._children.indexOf(top_node_join_child)
    joinNodeList(topIdx)._children(top_node_join_child_idx) = bottom_node_join_child

    // top_node.join_child._parent = top_node
    bottom_node_join_child._parent(0) = joinNodeList(topIdx)

    // bottom_node.join_child = top_node
    var bottom_node_join_child_idx = joinNodeList(bottomIdx)._children.indexOf(bottom_node_join_child)
    joinNodeList(bottomIdx)._children(bottom_node_join_child_idx) = joinNodeList(topIdx)

    // update output col
    var orig_output = joinNodeList(topIdx)._outputCols.clone()
    top_node_join_child._outputCols = orig_output

    for (o_col <- joinNodeList(topIdx)._children(top_node_join_child_idx)._outputCols) {
      //joinNodeList(topIdx)._children is now referring to the children of the prev bottom join node
      if (!joinNodeList(topIdx)._outputCols.contains(o_col) && isFromInputColumnSources(joinNodeList(topIdx), o_col)) {
        joinNodeList(topIdx)._outputCols += o_col
      }
    }

    // recurrsively check if any cols in top_node.join_child to add to top_node's outputCols
    // recursively update parents' output column list
    var node_iter = joinNodeList(topIdx)
    while (node_iter.nodeType == "JOIN_INNER") { //head of cascaded join
      var updated_ouput_list = new ListBuffer[String]
      for (o_col <- node_iter._outputCols) {
        if (isFromInputColumnSources(node_iter, o_col)) {
          updated_ouput_list += o_col
        }
      }
      for (o_col <- orig_output) {
        if (isFromInputColumnSources(node_iter, o_col) && !updated_ouput_list.contains(o_col)) {
          updated_ouput_list += o_col
        }
      }
      node_iter._outputCols = updated_ouput_list
      node_iter = node_iter._parent(0) // iterate to next parent
    }

    // recurrsively update tree depth
    joinNodeList(topIdx)._treeDepth = joinNodeList(topIdx)._parent(0)._treeDepth
    var prevNode = joinNodeList(topIdx)
    var nextNode = joinNodeList(topIdx)._parent(0)
    while(prevNode._treeDepth <= nextNode._treeDepth) {
      nextNode._treeDepth = prevNode._treeDepth - 1
      for (ch <- nextNode._children) {
        ch._treeDepth = nextNode._treeDepth + 1
      }
      prevNode = nextNode
      nextNode = nextNode._parent(0)
    }
  }

  def filterPredicatePushDown(joinNodeList: ListBuffer[SQL2FPGA_QPlan], dfmap: Map[String, DataFrame]): Unit ={
    println("Output: " + joinNodeList.head._outputCols)
    print("\n")

    var filterJoinNodeIdx = 1
    var nthFilterNode = 0
    var nthFilterNodeIdx = -1
    var deepestPushDownDepth = joinNodeList.length-1
    do {
      println("\n Searching for #" + filterJoinNodeIdx + " filter join node. #" + (filterJoinNodeIdx-1).toString + " cannot be pushed down")
      // reset
      nthFilterNode = 0
      nthFilterNodeIdx = -1
      deepestPushDownDepth = joinNodeList.length-1

      // find the filterJoinNodeIdx-th join node with a filter child
      for (join_node <- joinNodeList) {
        if (nthFilterNode < filterJoinNodeIdx && (join_node._children.head._nodeType == "Filter" || join_node._children.last._nodeType == "Filter")) {
          nthFilterNode += 1
          nthFilterNodeIdx = joinNodeList.indexOf(join_node)
        }
      }

      if (nthFilterNodeIdx == -1) {
        println("No filter join node")
        // sweep through the none-head node to make sure all cascade join paths are covered
        for (node_idx <- 1 to joinNodeList.length-1) {
          joinNodeList(node_idx).pushDownJoinWithFiltering(joinNodeList(node_idx), dfmap)
        }
        return
      }
      else if (nthFilterNodeIdx == joinNodeList.length - 1 || nthFilterNodeIdx == joinNodeList.length - 2) {
        println("Filter join node is already at the deepest depth")
        // sweep through the none-head node to make sure all cascade join paths are covered
        for (node_idx <- 1 to joinNodeList.length-1) {
          joinNodeList(node_idx).pushDownJoinWithFiltering(joinNodeList(node_idx), dfmap)
        }
        return
      }
      println("#" + filterJoinNodeIdx + " filter node idx: " + nthFilterNodeIdx)
      println(joinNodeList(nthFilterNodeIdx)._joining_expression.toString())

      for ( ref <- joinNodeList(nthFilterNodeIdx)._joining_expression.head.references) {
        var fromFilterNode = false
        for (ch <- joinNodeList(nthFilterNodeIdx)._children) {
          if (ch._nodeType == "Filter" && ch._outputCols.contains(ref.toString)) {
            fromFilterNode = true
          }
        }
        if (fromFilterNode) {
          println(ref.toString + " from filter node: " + fromFilterNode)
        }
        else {
          var firstOccurance = true
          var deepestPushDownDepth_this = nthFilterNodeIdx
          for( idx <- (joinNodeList.length-1) to nthFilterNodeIdx by -1) {
            if (joinNodeList(idx)._outputCols.contains(ref.toString) && firstOccurance == true) {
              firstOccurance = false
              deepestPushDownDepth_this = idx
            }
          }
          println(ref.toString + " from join node at depth: " + deepestPushDownDepth_this)
          deepestPushDownDepth = min(deepestPushDownDepth, deepestPushDownDepth_this)
        }
      }
      deepestPushDownDepth = deepestPushDownDepth - 1
      println("Deepest pushDown depth: " + deepestPushDownDepth)

      filterJoinNodeIdx += 1
    } while (nthFilterNodeIdx == deepestPushDownDepth);

    println("\n Pushing [" + nthFilterNodeIdx.toString + "] join node down to be the [" + deepestPushDownDepth.toString + "] join node")

    InsertTopBeforeBottom(joinNodeList, nthFilterNodeIdx, deepestPushDownDepth) // working version
  }

  def pushDownJoinWithFiltering(thisNode: SQL2FPGA_QPlan, dfmap: Map[String, DataFrame]): Unit = {
    // analysis
    var (maxCascadeJoinLength, maxCascadeJoinOperations, maxCascadeJoinFilter, maxCascadeJoinNode) = getMaxCascadeJoinChain(thisNode, 0)
    println("Max number of cascaded inner join in query: " + maxCascadeJoinLength)
    println("Cascaded inner join operations: ")
    print("\n")
    for (join_expr <- maxCascadeJoinOperations) {
      println("    " + join_expr.toString)
    }
    print("\n")
    for (join_filter <- maxCascadeJoinFilter) {
      println("    " + join_filter)
    }
    print("\n")
    for (join_node <- maxCascadeJoinNode) {
      println("    " + join_node._treeDepth)
    }
    print("\n")
    // transformation
    if (maxCascadeJoinNode.length > 2) { //FIXME: this should probably be "> 1"
      filterPredicatePushDown(maxCascadeJoinNode, dfmap)
    }
  }

  def joinReordering(joinNodeList: ListBuffer[SQL2FPGA_QPlan], dfmap: Map[String, DataFrame], sf: Int): Boolean ={
    // table gathering all input tables
    var childrenNode = new ListBuffer[SQL2FPGA_QPlan]
    var allKeyCol = new ListBuffer[String]
    var joinNodeCost = collection.mutable.SortedMap[Int, Int]()
    var joinNodeIdx = 0
    for (join_node <- joinNodeList) {
      // collect all non-join children
      for (ch <- join_node._children) {
        if (ch._nodeType != "JOIN_INNER") {
          childrenNode += ch
        }
      }
      // print out information
      print("    " + joinNodeIdx + ": ")
      for (ref_col <- join_node._joining_expression(0).references) {
        allKeyCol += ref_col.toString
      }
      var join_key_list = join_node.getJoinKeyTerms(join_node._joining_expression(0), false)
      var costScaling = join_key_list.length // more join pairs higher cost
      var costSum = 0
      for (key_pair <- join_key_list) {
        print("- " + key_pair + " - ")
        var key_pair_formatted = key_pair.split(" ").to(ListBuffer)
        key_pair_formatted.remove(1, 1) // "=" or "!=" in key pair term
        for (key <- key_pair_formatted) {
          print("(key: " + key + " table: " + columnDictionary(key) + " #row: " + getTableRow(columnDictionary(key)._1, sf) + ") ")
          costSum = costSum + getTableRow(columnDictionary(key)._1, sf)
        }
      }
      joinNodeCost += ((costSum * costScaling) -> joinNodeIdx)
      joinNodeIdx = joinNodeIdx + 1
      print("\n")
    }
    // print all join node costs
    for (cost <- joinNodeCost) {
      println("    " + cost._2 + ": " + cost._1)
    }
    // print all non-join children output col
    for (non_join_ch <- childrenNode) {
      println("    " + non_join_ch._outputCols)
    }
    // print the join chain required output cols
    print("\n")
    println(joinNodeList.last._outputCols)
    print("\n")

    var joinNodeOrderIdx = new ListBuffer[Int]
    for (cost <- joinNodeCost) {
      print("    " + cost._2 + ": " + cost._1)
      var joinNode = joinNodeList(cost._2)
      joinNodeOrderIdx += cost._2
      var join_key_list = joinNode.getJoinKeyTerms(joinNode._joining_expression(0), false)
      for (key_pair <- join_key_list) {
        print(" - " + key_pair + " - ")
        var key_pair_formatted = key_pair.split(" ").to(ListBuffer)
        key_pair_formatted.remove(1, 1) // "=" or "!=" in key pair term
        for (key <- key_pair_formatted) {
          var tmp_ch = childrenNode(0)
          for (ch <- childrenNode) {
            if (ch._outputCols.contains(key)) {
              tmp_ch = ch
            }
          }
          print(" key: " + key + " <- " + tmp_ch._outputCols + ",")
        }
      }
      print("\n")
    }
    print("\n")

    // parent and output cols of the last join node
    var parentOfLastJoinNode = joinNodeList.last._parent.clone()
    var outputOfLastJoinNode = joinNodeList.last._outputCols.clone()

    // bottom join node
    var bottomJoinNode = joinNodeList(joinNodeOrderIdx(0))
    var bottom_join_key_list = bottomJoinNode.getJoinKeyTerms(bottomJoinNode._joining_expression(0), false)
    var bottom_join_children = new ListBuffer[SQL2FPGA_QPlan]
    for (key_pair <- bottom_join_key_list) {
      var key_pair_formatted = key_pair.split(" ").to(ListBuffer)
      key_pair_formatted.remove(1, 1) // "=" or "!=" in key pair term
      for (key <- key_pair_formatted) {
        var tmp_ch = childrenNode(0)
        for (ch <- childrenNode) {
          if (ch._outputCols.contains(key)) {
            tmp_ch = ch
          }
        }
        bottom_join_children += tmp_ch
      }
    }
    bottomJoinNode._children = bottom_join_children
    // node output
    allKeyCol.clear()
    for (joinNodeIdx <- 1 to joinNodeCost.size-1) {
      var iterJoinNode = joinNodeList(joinNodeOrderIdx(joinNodeIdx))
      for (ref_col <- iterJoinNode._joining_expression(0).references) {
        if (!allKeyCol.contains(ref_col.toString)) {
          allKeyCol += ref_col.toString
        }
      }
    }
    for (o_col <- outputOfLastJoinNode) {
      if (!allKeyCol.contains(o_col)) {
        allKeyCol += o_col
      }
    }
    bottomJoinNode._outputCols.clear()
    for (ch <- bottomJoinNode._children) {
      for (o_col <- ch._outputCols) {
        if (allKeyCol.contains(o_col) && !bottomJoinNode._outputCols.contains(o_col)) {
          bottomJoinNode._outputCols += o_col
        }
      }
    }
    // node children linking
    for (ch <- bottomJoinNode._children) {
      ch._parent(0) = bottomJoinNode
    }
    // middle join nodes
    for (joinNodeIdx <- 1 to joinNodeCost.size-1) {
      var prev_joinNode = joinNodeList(joinNodeOrderIdx(joinNodeIdx-1))
      var middleJoinNode = joinNodeList(joinNodeOrderIdx(joinNodeIdx))
      var middle_join_key_list = middleJoinNode.getJoinKeyTerms(middleJoinNode._joining_expression(0), false)
      var middle_join_children = new ListBuffer[SQL2FPGA_QPlan]
      for (key_pair <- middle_join_key_list) {
        var key_pair_formatted = key_pair.split(" ").to(ListBuffer)
        key_pair_formatted.remove(1, 1) // "=" or "!=" in key pair term
        for (key <- key_pair_formatted) {
          var tmp_ch = childrenNode(0)
          for (ch <- childrenNode) {
            if (ch._outputCols.contains(key)) {
              tmp_ch = ch
            }
          }
          if (prev_joinNode._outputCols.contains(key)) {
            tmp_ch = prev_joinNode
          }
          if (!middle_join_children.contains(tmp_ch)) { // to avoid adding repeating nodes
            middle_join_children += tmp_ch
          }
        }
      }
      if (!middle_join_children.contains(prev_joinNode)) {
        return false
      }

      middleJoinNode._children = middle_join_children

      // node output
      if (joinNodeIdx < joinNodeCost.size-1) { // last node does not need this
        allKeyCol.clear()
        for (joinNodeIdx_inner <- joinNodeIdx+1 to joinNodeCost.size-1) {
          var iterJoinNode = joinNodeList(joinNodeOrderIdx(joinNodeIdx_inner))
          for (ref_col <- iterJoinNode._joining_expression(0).references) {
            if (!allKeyCol.contains(ref_col.toString)) {
              allKeyCol += ref_col.toString
            }
          }
        }
        for (o_col <- outputOfLastJoinNode) {
          if (!allKeyCol.contains(o_col)) {
            allKeyCol += o_col
          }
        }
        middleJoinNode._outputCols.clear()
        for (ch <- middleJoinNode._children) {
          for (o_col <- ch._outputCols) {
            if (allKeyCol.contains(o_col) && !middleJoinNode._outputCols.contains(o_col)) {
              middleJoinNode._outputCols += o_col
            }
          }
        }
      }

      for (ch <- middleJoinNode._children) {
        ch._parent(0) = middleJoinNode
      }
    }

    // top join node
    var topJoinNode = joinNodeList(joinNodeOrderIdx.last)
    topJoinNode._parent = parentOfLastJoinNode
    topJoinNode._outputCols = outputOfLastJoinNode

    var tmp_idx = 0
    var insertIdx = 0
    for (ch <- topJoinNode._parent(0)._children) {
      if (ch._nodeType == "JOIN_INNER") {
        insertIdx = tmp_idx
      }
      tmp_idx = tmp_idx + 1
    }
    topJoinNode._parent(0)._children(insertIdx) = topJoinNode

    return true
  }

  def pushDownJoinWithLessNumRows(thisNode: SQL2FPGA_QPlan, qNum: Int, dfmap: Map[String, DataFrame], sf: Int): Boolean = {
    // analysis
    var newList = new ListBuffer[SQL2FPGA_QPlan]
    var allCascadeJoinChains = getAllCascadeJoinChains(thisNode, newList)
    var uniqueStartingValandCount = collection.mutable.Map[Int, Int]()
    for (joinChain <- allCascadeJoinChains) {
      if (!uniqueStartingValandCount.contains(joinChain(0)._treeDepth)) {
        uniqueStartingValandCount += (joinChain(0)._treeDepth -> joinChain.length)
      } else {
        // Update to longer length
        if (joinChain.length > uniqueStartingValandCount(joinChain(0)._treeDepth)) {
          uniqueStartingValandCount(joinChain(0)._treeDepth) = joinChain.length
        }
      }
    }
    var allCascadeJoinChains_trimmed = new ListBuffer[ListBuffer[SQL2FPGA_QPlan]]
    for (joinChain <- allCascadeJoinChains) {
      if (uniqueStartingValandCount(joinChain(0)._treeDepth) == joinChain.length && !allCascadeJoinChains_trimmed.contains(joinChain)) {
        allCascadeJoinChains_trimmed += joinChain
      }
    }
    // transformation
    if (qNum != 2) {
      for (cascadeJoinChain <- allCascadeJoinChains_trimmed) {
        if (cascadeJoinChain.length >= 2) {
          var valid = joinReordering(cascadeJoinChain.reverse, dfmap, sf)
          if (valid == false)
            return false
        }
      }
    }
    else {
      // temp hack - Alec // join node reordering hack for Q2
      joinReordering(allCascadeJoinChains_trimmed(1).reverse, dfmap, sf)
      var filterNode = getParentsOtherChild(allCascadeJoinChains_trimmed(0).last._parent(0), allCascadeJoinChains_trimmed(0).last)
      var AggrNode = filterNode._children.head
      var joinNode = AggrNode._children.head
      joinNode._outputCols += "ps_suppkey#306"
      allCascadeJoinChains_trimmed(0).last._children(1) = joinNode
      AggrNode._children(0) = allCascadeJoinChains_trimmed(0).last
      allCascadeJoinChains_trimmed(0).last._outputCols += "ps_partkey#305"
    }

    return true
  }

  def applyCascadedJoinOptTransform(qParser: SQL2FPGA_QParser, qNum: Int, qConfig: SQL2FPGA_QConfig, dfmap: Map[String, DataFrame]): Unit = {
    // Opt 1: push down join with filtering child(ren)
    qParser.qPlan.pushDownJoinWithFiltering(qParser.qPlan, dfmap)
    qParser.qPlan_backup.pushDownJoinWithFiltering(qParser.qPlan_backup, dfmap)
    if (DEBUG_CASCADE_JOIN_OPT == true) {
      qParser.qPlan.printPlan_InOrder(dfmap)
      println(" ")
    }
    // Opt 2: reorder cascaded joins such that tables with lesser num of rows execute first
    var transformationSuccess = qParser.qPlan.pushDownJoinWithLessNumRows(qParser.qPlan, qNum, dfmap, qConfig.scale_factor)
    if (transformationSuccess == false) {
      qParser.qPlan = qParser.qPlan_backup
    }
    if (DEBUG_CASCADE_JOIN_OPT == true) {
      qParser.qPlan.printPlan_InOrder(dfmap)
      println(" ")
    }
  }

  def removeInvalidProjectFilter(dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit = {
    var q_node = Queue[SQL2FPGA_QPlan](this)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      // Breath-First Traversal
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        println(this_node._nodeType)

        if (this_node._nodeType == "Filter") {
          var onlyContainsIsNotNull = isAllFilterClausesIsNotNull(this_node._filtering_expression)
          var idx_this_node = -1
          if (this_node._parent.nonEmpty) {
            idx_this_node = this_node._parent(0)._children.indexOf(this_node)
          }
          if (onlyContainsIsNotNull) {
            if (this_node._children.length == 1) { // Normal cases
              this_node._parent(0)._children(idx_this_node) = this_node._children(0)
              this_node._children(0)._parent(0) = this_node._parent(0)
            }
            else { // Invalid cases
              this_node._parent(0)._children(idx_this_node) = this_node._children(-1)
            }
          }
        }

        if (this_node._nodeType == "Project") {
          //TODO: add logic to remove pass-through/pure-alising 'project'
          var onlyContainsAliasProjection = true
          for (aggr_expr <- this_node._aggregate_expression) {
            onlyContainsAliasProjection = onlyContainsAliasProjection & isOnlyContainsAliasProjection(aggr_expr)
          }
          var idx_this_node = -1
          if (this_node._parent.nonEmpty) {
            idx_this_node = this_node._parent(0)._children.indexOf(this_node)
          } else {
            return
          }
          if (onlyContainsAliasProjection) {
            if (this_node._children.length == 1) { // Normal cases
              this_node._children(0)._outputCols_alias = this_node._children(0)._outputCols
              this_node._children(0)._outputCols = this_node._outputCols
              this_node._children(0)._treeDepth = this_node._treeDepth
              this_node._children(0)._parent = this_node._parent
              this_node._parent(0)._children(idx_this_node) = this_node._children(0)
            }
            else { // Invalid cases
              this_node._parent(0)._children(idx_this_node) = this_node._children(-1)
            }
          }
        }

        for (ch <- this_node._children) {
          if (ch._parent.isEmpty)
            ch._parent += this_node
          tmp_q.enqueue(ch)
        }
      }
      println("")
      q_node = tmp_q
    }
  }

  def resetVisitTag_QPlan(rootNode: SQL2FPGA_QPlan): Unit = {
    if (rootNode == null) return

    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(rootNode)

    while(!q_node.isEmpty) {
      var this_node = q_node.dequeue()
      // reset visit tag
      this_node.genHostCodeVisited = false
      this_node.genSWConfigCodeVisited = false
      this_node.genFPGAConfigCodeVisited = false
      // enqueue children nodes
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
    }
  }

  def applyRedundantNodeRemovalOptTransform(qParser: SQL2FPGA_QParser, qNum: Int, qConfig: SQL2FPGA_QConfig, dfmap: Map[String, DataFrame]): Unit = {
    qParser.qPlan.removeInvalidProjectFilter(dfmap, qConfig.pure_sw_mode)
    if (DEBUG_REDUNDANT_OP_OPT) {
      println(" AFTER removeInvalidProjectFilter()")
      qParser.qPlan.printPlan_InOrder(dfmap)
    }
    qParser.qPlan.allocateOperators(qNum, dfmap, qConfig.pure_sw_mode)
    qParser.qPlan.eliminatedRepeatedNode(qParser.qPlan)
    resetVisitTag_QPlan(qParser.qPlan)
    if (DEBUG_REDUNDANT_OP_OPT) {
      println(" AFTER eliminatedRepeatedNode()")
      qParser.qPlan.printPlan_InOrder(dfmap)
    }
  }

  def convertOuterJoin(dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit = {
    var q_node = Queue[SQL2FPGA_QPlan](this)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      // Breath-First Traversal
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        println(this_node._nodeType)

        if (this_node._nodeType == "JOIN_LEFTOUTER") {
          var newJoinNode = new SQL2FPGA_QPlan
          newJoinNode._nodeType = "JOIN_INNER"
          newJoinNode._children = this_node._children.clone()
          newJoinNode._parent += this_node
          var newOutputList = new ListBuffer[String]
          for (ch <- this_node._children) {
            for (o_col <- ch._outputCols) {
              newOutputList += o_col
            }
          }
          newJoinNode._outputCols = newOutputList
          newJoinNode._operation = this_node._operation.clone()
          newJoinNode._joining_expression = this_node._joining_expression.clone()

          this_node._nodeType = "JOIN_LEFTANTI"
          var newChildrenList = new ListBuffer[SQL2FPGA_QPlan]
          if (pure_sw_mode == 1) {
            newChildrenList += this_node._children(0)
            newChildrenList += newJoinNode
          } else {
            newChildrenList += newJoinNode
            newChildrenList += this_node._children(0)
          }
          this_node._children = newChildrenList

          // TODO: this is not completed - need to add a SW function node that concatnate the inner+anti join outputs
        }

        for (ch <- this_node._children) {
          if (ch._parent.isEmpty)
            ch._parent += this_node
          tmp_q.enqueue(ch)
        }
      }
      println("")
      q_node = tmp_q
    }
  }

  def updateTreeDepth(root: SQL2FPGA_QPlan): Unit = {
    var q_node = Queue[SQL2FPGA_QPlan](root)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      // Breath-First Traversal
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        // Tree depth increase by 1
        this_node._treeDepth = this_node._treeDepth + 1
        for (ch <- this_node._children) {
          tmp_q.enqueue(ch)
        }
      }
      q_node = tmp_q
    }
  }

  def eliminateSubPerfOverlay(root: SQL2FPGA_QPlan): Unit = {
    var q_node = Queue[SQL2FPGA_QPlan](root)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      // Breath-First Traversal
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        //isolated single "filter" node - much faster done on CPU
        if (this_node._cpuORfpga == 1) {
          if (this_node._nodeType == "Filter") {
            if (this_node._bindedOverlayInstances.isEmpty) {
              this_node._cpuORfpga = 0
            }
          }
        }
        for (ch <- this_node._children) {
          tmp_q.enqueue(ch)
        }
      }
      q_node = tmp_q
    }
  }

  def applyFPGAOverlayOptTransform(qParser: SQL2FPGA_QParser, qConfig: SQL2FPGA_QConfig, dfmap: Map[String, DataFrame]): (Int, Int) = {
    var (num_over_orig, num_over_fused) = qParser.qPlan.optimizeFPGAOperator(dfmap)
    qParser.qPlan.eliminateSubPerfOverlay(this)
    return (num_over_orig, num_over_fused)
  }

  def convertSpecialAntiJoin(dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit = {
    var q_node = Queue[SQL2FPGA_QPlan](this)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      // Breath-First Traversal
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        println(this_node._nodeType)

        if (this_node._nodeType == "JOIN_LEFTANTI" && this_node._isSpecialAntiJoin) {
          var newJoinNode = new SQL2FPGA_QPlan
          newJoinNode._nodeType = "JOIN_LEFTSEMI"
          newJoinNode._treeDepth = this_node._treeDepth
          newJoinNode._isSpecialSemiJoin = true
          var newJoinNodeChildrenList = new ListBuffer[SQL2FPGA_QPlan]
          for (ch <- this_node._children) {
            newJoinNodeChildrenList += ch.deepCopy()
          }
          newJoinNode._children = newJoinNodeChildrenList
          updateTreeDepth(newJoinNode)
          newJoinNode._parent += this_node
          var newOutputList = new ListBuffer[String]
          for (o_col <- this_node._children(0)._outputCols) {
            newOutputList += o_col
          }
          newJoinNode._outputCols = newOutputList
          newJoinNode._operation = this_node._operation.clone()
          newJoinNode._joining_expression = this_node._joining_expression.clone()

          this_node._nodeType = "JOIN_LEFTANTI"
          var newChildrenList = new ListBuffer[SQL2FPGA_QPlan]
          newChildrenList += this_node._children(1)
          newChildrenList += newJoinNode
          this_node._children = newChildrenList
        }

        for (ch <- this_node._children) {
          if (ch._parent.isEmpty)
            ch._parent += this_node
          tmp_q.enqueue(ch)
        }
      }
      println("")
      q_node = tmp_q
    }
  }

  def applySpecialJoinOptTransform(qParser: SQL2FPGA_QParser, qConfig: SQL2FPGA_QConfig, dfmap: Map[String, DataFrame]): Unit = {
    // TODO: this is incomplete: need to concat. inner_join and anti_join outputs as one output
    qParser.qPlan.convertOuterJoin(dfmap, qConfig.pure_sw_mode)
    if (DEBUG_SPECIAL_JOIN_OPT) {
      println(" AFTER convertOuterJoin()")
      qParser.qPlan.printPlan_InOrder(dfmap)
    }
    qParser.qPlan.convertSpecialAntiJoin(dfmap, qConfig.pure_sw_mode)
    if (DEBUG_SPECIAL_JOIN_OPT) {
      println(" AFTER convertSpecialAntiJoin()")
      qParser.qPlan.printPlan_InOrder(dfmap)
    }
  }

  def allocateOperators(qNum: Int, dfmap: Map[String, DataFrame], pure_sw_mode: Int): Unit = {
    println(" ")
    this.printPlan_InOrder(dfmap)
    println(" ")

    var q_node = Queue[SQL2FPGA_QPlan](this)
    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      // Breath-First Traversal
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        println(this_node._nodeType)

        var cpuORfpgaExecution = 1 // CPU-0, FPGA-1
        var overlay_type = 0 // gqe_join-0, gqe_aggr-1, gqe_part-2
        if (this_node._children.isEmpty) { // leaf node
          for (i_col <- this_node._inputCols){
            var tbl_col_type = getColumnType(i_col, dfmap)
            // current Xilinx DB tool does not support string datatype
            // if (tbl_col_type == "StringType") {
            if (tbl_col_type == "StringType" && this_node._stringRowIDSubstitution == false) {
              cpuORfpgaExecution = 0
            }
          }
          overlay_type = this_node._parent(0)._fpgaOverlayType
          cpuORfpgaExecution = this_node._parent(0)._cpuORfpga
        }
        else { // non-leaf node
          // if the number output columns is more than 8, execute this node on CPU
          if (this_node._outputCols.length > 8) {
            // below was to enforce 8 output columns
            var isGroupByOp = this_node._nodeType == "Aggregate" && this_node._groupBy_operation.nonEmpty
            if (!isGroupByOp) {
              cpuORfpgaExecution = 0
            }
          }
          for (ch <- this_node._children) {
            // if the number input columns is more than 8, execute this node on CPU
            if (ch._outputCols.length > 8) {
              // below was to enforce 8 output columns
              cpuORfpgaExecution = 0
            }
            // if any input col is StringType, execute this node on CPU
            for (i_col <- ch._outputCols) {
              var tbl_col_type = getColumnType(i_col, dfmap)
              // if (tbl_col_type == "StringType") {
              if (tbl_col_type == "StringType" && (ch._stringRowIDSubstitution == false || this_node._stringRowIDSubstitution == false)) {
                cpuORfpgaExecution = 0
              }
            }
          }
          // if filter op and any input col that needs filtering is not in the first 4 cols
          if (this_node._nodeType == "Filter") {
            var clauseCount = getNumberofFilterClause(this_node._filtering_expression)
            var clauseReferenceCount = this_node._filtering_expression.references.size
            var clauseCountExceedLimit = clauseCount > 4 && clauseReferenceCount > 4
            var allValidClauses = isAllFilterClausesValid(this_node._filtering_expression)
            var includeSubQuery = this_node._children.length > 1
            if (clauseCountExceedLimit || !allValidClauses || includeSubQuery) { // overlays only support 4 filtering clauses
              cpuORfpgaExecution = 0
            }
            //FIXME: Alec-added temp test - come back here Alec hack
            cpuORfpgaExecution = 0 // disabling filtering during sf30
            if (!this_node._parent.isEmpty && this_node._parent(0)._nodeType.contains("JOIN") ) {
              if (this_node._parent(0)._nodeType != "JOIN_INNER") {
                cpuORfpgaExecution = 0
              }
            }
          }
          if (this_node._nodeType == "JOIN_INNER" || this_node._nodeType == "JOIN_LEFTANTI" ||
            this_node._nodeType == "JOIN_LEFTSEMI" || this_node._nodeType == "JOIN_LEFTOUTER") {
            var join_clauses = getJoinKeyTerms(this_node.joining_expression(0), false)
            // var invalid_join_connect = this_node._operation.head.contains("NOT") || this_node._operation.head.contains("OR")
            // var invalid_join_connect = (join_clauses.length > 1) & this_node._operation.head.contains("OR")
            var invalid_join_connect = this_node._operation.head.contains("OR") && this_node._nodeType != "JOIN_LEFTANTI"
            var isSpecialSemiJoin = this_node._nodeType == "JOIN_LEFTSEMI" && join_clauses.length == 2 &&
              ((join_clauses(0).contains("!=") && !join_clauses(1).contains("!=")) || (!join_clauses(0).contains("!=") && join_clauses(1).contains("!=")))
            this_node._isSpecialSemiJoin = isSpecialSemiJoin
            var isSpecialAntiJoin = this_node._nodeType == "JOIN_LEFTANTI" && join_clauses.length == 2 &&
              ((join_clauses(0).contains("!=") && !join_clauses(1).contains("!=")) || (!join_clauses(0).contains("!=") && join_clauses(1).contains("!=")))
            this_node._isSpecialAntiJoin = isSpecialAntiJoin
            var join_filter_clauses = getJoinFilterTerms(this_node.joining_expression(0), false)
            //no support for join term > 2, non-AND joining relation, filtering, and leftouter join
            // if (join_clauses.length > 2 || invalid_join_connect || join_filter_clauses.length != 0 || this_node._nodeType == "JOIN_LEFTOUTER") {
            if (join_clauses.length > 2 || invalid_join_connect || join_filter_clauses.length != 0) {
              cpuORfpgaExecution = 0
            }
            if (qNum == 2) {
              //temp hack - Alec hack for Q2
              if (this_node._treeDepth < 5) {
                cpuORfpgaExecution = 0
              }
            }
          }
          // if (this_node._nodeType == "Sort" || this_node._nodeType == "Project") {
          if (this_node._nodeType == "Sort") {
            cpuORfpgaExecution = 0
          }
          if (this_node._nodeType == "Aggregate") {
            // check all aggregation operation is supported
            var num_evaluation_expr = 0
            for (aggr_expr <- _aggregate_expression) {
              if (!isValidAggregateExpression(aggr_expr)) {
                cpuORfpgaExecution = 0
              }
              if (!isPureAggrOperation_sub(aggr_expr)) {
                num_evaluation_expr += 1
              }
            }
            // making sure there is no more than 2 aggr operations because there is only two eval units in the overlays
            if (num_evaluation_expr > 2) {
              cpuORfpgaExecution = 0
            }
            // support groupBy operation
            if (this_node._groupBy_operation.length > 0) {
              overlay_type = 1 // gqe_aggr-1
              // TODO: consider changing the gqe-aggr to support groupBy of non-aggregation-required cols (e.g., column aliasing)
              // TODO cont.: adding enums::AOP_LAST - always keeping the last occurance of the col element
              for (expr <- this_node._aggregate_expression) {
                if (isPureEvalOperation_sub(expr)) {
                  cpuORfpgaExecution = 0
                }
              }
            } else {
              // Alec hack - temporaily disabling aggregate offloading to gqejoin
              cpuORfpgaExecution = 0
            }
            // Alec hack
            // cpuORfpgaExecution = 0
          }
        }
        //Alec-added: implement every operation on CPU with set _cpuORfpga as 0
        if (pure_sw_mode == 1) {
          cpuORfpgaExecution = 0
        }

        this_node._cpuORfpga = cpuORfpgaExecution
        this_node._fpgaOverlayType = overlay_type

        for (ch <- this_node._children) {
          if (ch._parent.isEmpty)
            ch._parent += this_node
          tmp_q.enqueue(ch)
        }
      }
      println("")
      q_node = tmp_q
    }
    getOverlayCallCount()
    println("Number of gqe-join calls: " + this._fpgaJoinOverlayCallCount)
    println("Number of gqe-aggr calls: " + this._fpgaAggrOverlayCallCount)
    println("")
  }

  def isUniqueNode(datumNode: SQL2FPGA_QPlan): Boolean={
    var result = true

    var q_node = Queue[SQL2FPGA_QPlan]()
    var rootNode = this
    while(rootNode != null && rootNode._parent.nonEmpty) {
      rootNode = rootNode._parent(0)
    }
    //q_node.enqueue(getParentsOtherChild(datumNode, this)) // insert only the other children
    q_node.enqueue(rootNode) // insert the root node entire query plan

    while(!q_node.isEmpty) {
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      while(!q_node.isEmpty) {
        var this_node = q_node.dequeue()
        for (ch <- this_node._children) {
          // need to skip over 'this' node when starting from the root node of the entire query plan
          var isThisNode = (ch == this) && (ch._parent(0) == datumNode)
          if (!isThisNode) {
            // not same node, but same input cols, operations, and output cols
            var sameNode_basedOnOp = true
            // check input cols
            if (ch._children.length == this._children.length) {
              var inputCol_idx = 0
              for (i_col_ch <- ch._children) {
                if (i_col_ch._outputCols != this._children(inputCol_idx)._outputCols) {
                  // check if other child's input col list is a superset of this node's input col list
                  for (this_input_col <- this._children(inputCol_idx)._outputCols) {
                    if (!i_col_ch._outputCols.contains(this_input_col)) {
                      sameNode_basedOnOp = false
                    }
                  }
                }
                inputCol_idx = inputCol_idx + 1
              }
            }
            else {
              sameNode_basedOnOp = false
            }
            // check operations
            if (ch._operation != this._operation) {
              sameNode_basedOnOp = false
            }
            // check output cols
            if (ch._outputCols != this._outputCols) {
              // check if other child's output col list is a superset of this node's output col list
              for (this_output_col <- this._outputCols) {
                if (!ch._outputCols.contains(this_output_col)) {
                  sameNode_basedOnOp = false
                }
              }
            }
            // check device context - if same node after checking previous conditions
            if (sameNode_basedOnOp == true) {
              if (ch._cpuORfpga == 1 && this._cpuORfpga == 1) {
                if (ch._fpgaOverlayType != this._fpgaOverlayType) {
                  sameNode_basedOnOp = false
                }
              } else if ((ch._cpuORfpga == 0 && this._cpuORfpga == 1) ||
                (ch._cpuORfpga == 1 && this._cpuORfpga == 0)) {
                //                  ch._cpuORfpga = 1sameNode_basedOnOp = false
                //                  ch._parent(0) = this._parent(0)
                sameNode_basedOnOp = false
              }
            }
            // update node linking
            if (sameNode_basedOnOp == true) {
              var ch_idx = this._parent(0)._children.indexOf(this)
              if (ch_idx != -1) {
                var tmp_children_list = this._parent(0)._children.clone()
                tmp_children_list(ch_idx) = ch
                this._parent(0)._children = tmp_children_list
              }
            }
            // update final boolean result
            if (ch == this || sameNode_basedOnOp == true) {
              result = false
            }
            // insert bottom nodes
            tmp_q.enqueue(ch)
          }
        }
      }
      q_node = tmp_q
    }

    return result
  }

  def eliminatedRepeatedNode(rootNode: SQL2FPGA_QPlan): Unit ={
    if (rootNode == null) return

    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(rootNode)

    while(q_node.nonEmpty) {
      var this_node = q_node.dequeue()
      // update linking
      for (ch <- this_node._children) {
        if (!ch.genHostCodeVisited) {
          ch.genHostCodeVisited = true
          ch.isUniqueNode(this_node)
        }
      }
      // enqueue children nodes
      for (ch <- this_node._children) {
        q_node.enqueue(ch)
      }
    }
  }

  def genCode(parentNode: SQL2FPGA_QPlan, dfmap: Map[String, DataFrame], qConfig: SQL2FPGA_QConfig, queryNum: Int): Unit = {
    //-----------------------------------START-----------------------------------
    //-----------------------Recursive Call to Children Nodes--------------------
    for (ch <- _children) {
      ch.genCode(this, dfmap, qConfig, queryNum)
    }
    if (!_genCodeVisited) {
      _genCodeVisited = true
    } else {
      return
    }
    var sf = qConfig.scale_factor
    // Temp Hack Alec - tag:output_table_nrow
    queryNum match {
      case 1 =>
        if (_nodeType == "Aggregate" && _treeDepth == 1 && _cpuORfpga == 1) {
          _numTableRow = 10
        }
      case 2 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 7 && _cpuORfpga == 1) {
          _numTableRow = 5
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 8 && _cpuORfpga == 1) {
          _numTableRow = 2036
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 9 && _cpuORfpga == 1) {
          _numTableRow = 162880
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 5 && _cpuORfpga == 1) {
          _numTableRow = 628
        }
      case 3 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 150000
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 30000
        } else if (_nodeType == "Aggregate" && _treeDepth == 1 && _cpuORfpga == 1) {
          _numTableRow = 24000
        }
      case 4 =>
        if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 53000
        }
      case 5 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 7500
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 37000
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 37000
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 5 && _cpuORfpga == 1) {
          _numTableRow = 909000
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 227000
        }
      case 6 => // N/A
      case 7 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 153500
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 5 && _cpuORfpga == 1) {
          _numTableRow = 153500
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 153500
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 7 && _cpuORfpga == 1) {
          _numTableRow = 1829000
        }
      case 8 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 2539
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 2539
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 5 && _cpuORfpga == 1) {
          _numTableRow = 12215
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 12215
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 7 && _cpuORfpga == 1) {
          _numTableRow = 12215
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 8 && _cpuORfpga == 1) {
          _numTableRow = 39720
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 9 && _cpuORfpga == 1) {
          _numTableRow = 39720
        }
      case 9 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 319287
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 5 && _cpuORfpga == 1) {
          _numTableRow = 319287
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 319287
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 7 && _cpuORfpga == 1) {
          _numTableRow = 319287
        }
      case 10 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 114347
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 114347
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 57111
        }
      case 11 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 393
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 31440
        } else if (_nodeType == "Aggregate" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 60000
        }
      case 12 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 31211
        }
      case 13 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 0 && _cpuORfpga == 1) {
          _numTableRow = 1480133
        } else if (_nodeType == "JOIN_LEFTANTI" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 1531000
        }
      case 14 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 1 && _cpuORfpga == 1) {
          _numTableRow = 78000
        }
      case 15 =>
      case 16 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 120789
        } else if (_nodeType == "JOIN_LEFTANTI" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 799680
        }
      case 17 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 1507
        }
      case 18 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 100
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 100
        } else if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 100
        } else if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 100
        } else if (_nodeType == "Aggregate" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 3000000
        } else if (_nodeType == "Aggregate" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 3000000
        }
      case 19 =>
      case 20 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 1 && _cpuORfpga == 1) {
          _numTableRow = 210
        } else if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 5366
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 7670
        } else if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 11160
        } else if (_nodeType == "Aggregate" && _treeDepth == 5 && _cpuORfpga == 1) {
          _numTableRow = 15000
        } else if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 12816
        }
      case 21 =>
        if (_nodeType == "JOIN_INNER" && _treeDepth == 2 && _cpuORfpga == 1) {
          _numTableRow = 25255
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 377
        } else if (_nodeType == "JOIN_INNER" && _treeDepth == 4 && _cpuORfpga == 1) {
          _numTableRow = 52212
        } else if (_nodeType == "JOIN_LEFTANTI" && _treeDepth == 5 && _cpuORfpga == 1) {
          _numTableRow = 1375555
        } else if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 6 && _cpuORfpga == 1) {
          _numTableRow = 2417632
        } else if (_nodeType == "JOIN_LEFTSEMI" && _treeDepth == 7 && _cpuORfpga == 1) {
          _numTableRow = 2844584
        }
      case 22 =>
        if (_nodeType == "JOIN_LEFTANTI" && _treeDepth == 3 && _cpuORfpga == 1) {
          _numTableRow = 6283
        }
    }

    //-----------------------------------Node Operation Name------------------------------------------
    var nodeOpName = _nodeType + "_TD_" + _treeDepth + scala.util.Random.nextInt(1000)
    _fpgaNodeName = nodeOpName
    _fpgaCode += nodeOpName
    //-----------------------------------Execution Mode: CPU or FPGA----------------------------------
    _fpgaCode += "cpuORfpgaMode: " + _cpuORfpga.toString
    //-----------------------------------INPUT TABLE & COLUMN-----------------------------------------
    if (_children.isEmpty) {
      // This assumes the node is "SerializeFromObject"
      // read _inputCols.length number of columns
      // from a single table which can be looked up using "columnDictionary"
      var inputTblCode = new ListBuffer[String]()

      //TODO: fix input table gen after prun away invalid stringRowIDSubstitution table
      //tag:table_gen
      if (_stringRowIDSubstitution == true) {
        //Substitute table - table with row_id
        var tbl_name = "tbl_" + nodeOpName + "_input" + "_stringRowIDSubstitute"
        _fpgaInputTableName = tbl_name
        var tbl = columnDictionary(_inputCols.head)._1
        var num_cols = _inputCols.length
        inputTblCode += "Table " + tbl_name + ";"
        inputTblCode += tbl_name + " = Table(\"" + tbl + "\", " + tbl + "_n, " + num_cols + ", in_dir);"
        for (col_name <- _inputCols) {
          var col = columnDictionary(col_name)._2
          var col_type = getColumnDataType(dfmap(tbl), col)
          var col_type_text = "4"
          if (col_type == "StringType"){
            inputTblCode += tbl_name + ".addCol(\"" + col + "\", 4, 1, 0);"
          }
          else {
            inputTblCode += tbl_name + ".addCol(\"" + col + "\", 4);"
          }
        }
        inputTblCode += tbl_name + ".allocateHost();"
        inputTblCode += tbl_name + ".loadHost();"
        //Substitute table - original table with string
        if (_cpuORfpga == 1) {
          if (parentNode._cpuORfpga == 1 && parentNode._fpgaOverlayType == 0) {
            if (sf == 30) {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_h, 33);"
              var part_tbl_name = tbl_name + "_partition"
              var part_tbl_array_name = tbl_name + "_partition_array"
              inputTblCode += "Table " + part_tbl_name + "(\"" + part_tbl_name + "\", " + tbl + "_n, " + num_cols + ", \"\");"
              inputTblCode += part_tbl_name + ".allocateHost(1.2, hpTimes_join);"
              inputTblCode += part_tbl_name + ".allocateDevBuffer(context_h, 32);"
              inputTblCode += "Table " + part_tbl_array_name + "[hpTimes_join];"
              inputTblCode += "for (int i(0); i < hpTimes_join; ++i) {"
              inputTblCode += "    " + part_tbl_array_name + "[i] = " + part_tbl_name + ".createSubTable(i);"
              inputTblCode += "}"
            } else {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_h, 32);"
            }
          }
          else if (parentNode._cpuORfpga == 1 && parentNode._fpgaOverlayType == 1) {
            if (sf == 30) {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_a, 33);"
              var part_tbl_name = tbl_name + "_partition"
              var part_tbl_array_name = tbl_name + "_partition_array"
              inputTblCode += "Table " + part_tbl_name + "(\"" + part_tbl_name + "\", " + tbl + "_n, " + num_cols + ", \"\");"
              inputTblCode += part_tbl_name + ".allocateHost(1.2, hpTimes_aggr);"
              inputTblCode += part_tbl_name + ".allocateDevBuffer(context_a, 32);"
              inputTblCode += "Table " + part_tbl_array_name + "[hpTimes_aggr];"
              inputTblCode += "for (int i(0); i < hpTimes_aggr; ++i) {"
              inputTblCode += "    " + part_tbl_array_name + "[i] = " + part_tbl_name + ".createSubTable(i);"
              inputTblCode += "}"
            } else {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_a, 32);"
            }
          }
        }
        //Substitute table - original table with string
        tbl_name = "tbl_" + nodeOpName + "_input"
        _fpgaInputTableName_stringRowIDSubstitute = tbl_name
        tbl = columnDictionary(_inputCols.head)._1
        num_cols = _inputCols.length
        inputTblCode += "Table " + tbl_name + ";"
        inputTblCode += tbl_name + " = Table(\"" + tbl + "\", " + tbl + "_n, " + num_cols + ", in_dir);"
        for (col_name <- _inputCols){
          var col = columnDictionary(col_name)._2
          var col_type = getColumnDataType(dfmap(tbl), col)
          var col_type_text = "4"
          if (col_type == "StringType"){
            var tbl_col = (tbl, col)
            col_type_text = getStringLengthMacro(tbl_col) + "+1"
          }
          inputTblCode += tbl_name + ".addCol(\"" + col + "\", " + col_type_text + ");"
        }
        inputTblCode += tbl_name + ".allocateHost();"
        inputTblCode += tbl_name + ".loadHost();"
      }
      else {
        var tbl_name = "tbl_" + nodeOpName + "_input"
        _fpgaInputTableName = tbl_name
        var tbl = columnDictionary(_inputCols.head)._1
        var num_cols = _inputCols.length
        inputTblCode += "Table " + tbl_name + ";"
        inputTblCode += tbl_name + " = Table(\"" + tbl + "\", " + tbl + "_n, " + num_cols + ", in_dir);"
        for (col_name <- _inputCols){
          var col = columnDictionary(col_name)._2
          var col_type = getColumnDataType(dfmap(tbl), col)
          var col_type_text = "4"
          if (col_type == "StringType") {
            var tbl_col = (tbl, col)
            col_type_text = getStringLengthMacro(tbl_col) + "+1"
          }
          inputTblCode += tbl_name + ".addCol(\"" + col + "\", " + col_type_text + ");"
        }
        inputTblCode += tbl_name + ".allocateHost();"
        inputTblCode += tbl_name + ".loadHost();"

        if (_cpuORfpga == 1) {
          if (parentNode._cpuORfpga == 1 && parentNode._fpgaOverlayType == 0) {
            if (sf == 30) {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_h, 33);"
              var part_tbl_name = tbl_name + "_partition"
              var part_tbl_array_name = tbl_name + "_partition_array"
              inputTblCode += "Table " + part_tbl_name + "(\"" + part_tbl_name + "\", " + tbl + "_n, " + num_cols + ", \"\");"
              inputTblCode += part_tbl_name + ".allocateHost(1.2, hpTimes_join);"
              inputTblCode += part_tbl_name + ".allocateDevBuffer(context_h, 32);"
              inputTblCode += "Table " + part_tbl_array_name + "[hpTimes_join];"
              inputTblCode += "for (int i(0); i < hpTimes_join; ++i) {"
              inputTblCode += "    " + part_tbl_array_name + "[i] = " + part_tbl_name + ".createSubTable(i);"
              inputTblCode += "}"
            } else {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_h, 32);"
            }
          }
          else if (parentNode._cpuORfpga == 1 && parentNode._fpgaOverlayType == 1) {
            if (sf == 30) {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_a, 33);"
              var part_tbl_name = tbl_name + "_partition"
              var part_tbl_array_name = tbl_name + "_partition_array"
              inputTblCode += "Table " + part_tbl_name + "(\"" + part_tbl_name + "\", " + tbl + "_n, " + num_cols + ", \"\");"
              inputTblCode += part_tbl_name + ".allocateHost(1.2, hpTimes_aggr);"
              inputTblCode += part_tbl_name + ".allocateDevBuffer(context_a, 32);"
              inputTblCode += "Table " + part_tbl_array_name + "[hpTimes_aggr];"
              inputTblCode += "for (int i(0); i < hpTimes_aggr; ++i) {"
              inputTblCode += "    " + part_tbl_array_name + "[i] = " + part_tbl_name + ".createSubTable(i);"
              inputTblCode += "}"
            } else {
              _fpgaInputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_a, 32);"
            }
          }
        }
      }
      _fpgaInputCode = inputTblCode
      _fpgaCode += inputTblCode.toString()
    }
    else {
      for (ch <- _children) {
        // Get output tables from the children node(s) and use as input tables
        _fpgaInputCode ++= ch.fpgaOutputCode
        _fpgaInputDevAllocateCode ++= ch.fpgaOutputDevAllocateCode
        _fpgaCode ++= ch.fpgaOutputCode
      }
    }
    //-----------------------------------OUTPUT TABLE & COLUMN----------------------------------------
    if (_outputCols.nonEmpty) {
      if (_operation.isEmpty) {
        // This assumes the node is "SerializeFromObject", so outputCol equals inputCol
        _fpgaOutputTableName = _fpgaInputTableName
        _fpgaOutputTableName_stringRowIDSubstitute = _fpgaInputTableName_stringRowIDSubstitute
        _fpgaOutputCode = _fpgaInputCode
        _fpgaOutputDevAllocateCode = _fpgaInputDevAllocateCode
      }
      else {
        for (col <- _outputCols){
          if (columnDictionary.contains(col) == false){
            // TODO: Add updated expression
            columnDictionary += (col -> (_fpgaNodeName, null))
          }
        }
        var outputTblCol = new ListBuffer[String]()
        var max_num_rows = 6100000 * sf
        var tbl_name_fpga_table = "tbl_" + _fpgaNodeName + "_output" + "_preprocess"
        var tbl_name = "tbl_" + _fpgaNodeName + "_output"

        if (_cpuORfpga == 1) {
          max_num_rows = _numTableRow
        }

        var intermediate_num_cols = 0
        var part_tbl_name = ""
        var part_tbl_array_name = ""
        if (_cpuORfpga == 1 && _fpgaOverlayType == 0) { // gqe-join
          if (parentNode != null && parentNode.cpuORfpga == 1 && parentNode._fpgaOverlayType == 1) {
            _fpgaOutputTableName = tbl_name_fpga_table
            intermediate_num_cols = _outputCols.length
            outputTblCol += "Table " + _fpgaOutputTableName + "(\"" + _fpgaOutputTableName + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
            outputTblCol += _fpgaOutputTableName + ".allocateHost();"
            _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_h, 32);"
            // duplicate table for aggr overlay
            outputTblCol += "Table " + tbl_name + "(\"" + tbl_name + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
            outputTblCol += tbl_name + ".allocateHost();"
            parentNode._fpgaOutputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_a, 32);"
          }
          else {
            _fpgaOutputTableName = tbl_name
            intermediate_num_cols = _outputCols.length
            outputTblCol += "Table " + _fpgaOutputTableName + "(\"" + _fpgaOutputTableName + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
            if (sf == 30) {
              outputTblCol += _fpgaOutputTableName + ".allocateHost(1.2, hpTimes_join);"
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_h, 32);"
              part_tbl_array_name = tbl_name + "_partition_array"
              _fpgaOutputDevAllocateCode += "Table " + part_tbl_array_name + "[hpTimes_join];"
              _fpgaOutputDevAllocateCode += "for (int i(0); i < hpTimes_join; ++i) {"
              _fpgaOutputDevAllocateCode += "    " + part_tbl_array_name + "[i] = " + _fpgaOutputTableName + ".createSubTable(i);"
              _fpgaOutputDevAllocateCode += "}"
            } else {
              outputTblCol += _fpgaOutputTableName + ".allocateHost();"
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_h, 32);"
            }
          }
        }
        else if (_cpuORfpga == 1 && _fpgaOverlayType == 1) { // gqe-aggr
          if (parentNode != null && parentNode.cpuORfpga == 1 && parentNode._fpgaOverlayType == 0) {
            _fpgaOutputTableName = tbl_name_fpga_table
            intermediate_num_cols = 16 // gqe-outputs a 16-col tbl
            outputTblCol += "Table " + _fpgaOutputTableName + "(\"" + _fpgaOutputTableName + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
            outputTblCol += _fpgaOutputTableName + ".allocateHost();"
            _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_a, 33);"
            // add another output table, to shrink and shuffle 16-col table to an 8-col table, when using the gqe-aggr overlay
            outputTblCol += "Table " + tbl_name + "(\"" + tbl_name + "\", " + max_num_rows + ", " + _outputCols.length + ", \"\");"
            outputTblCol += tbl_name + ".allocateHost();"
            _fpgaOutputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_h, 32);"
          }
          else if (parentNode != null && parentNode.cpuORfpga == 1 && parentNode._fpgaOverlayType == 1) {
            _fpgaOutputTableName = tbl_name_fpga_table
            intermediate_num_cols = 16 // gqe-outputs a 16-col tbl
            outputTblCol += "Table " + _fpgaOutputTableName + "(\"" + _fpgaOutputTableName + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
            outputTblCol += _fpgaOutputTableName + ".allocateHost();"
            _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_a, 33);"
            // add another output table, to shrink and shuffle 16-col table to an 8-col table, when using the gqe-aggr overlay
            outputTblCol += "Table " + tbl_name + "(\"" + tbl_name + "\", " + max_num_rows + ", " + _outputCols.length + ", \"\");"
            outputTblCol += tbl_name + ".allocateHost();"
            _fpgaOutputDevAllocateCode += tbl_name + ".allocateDevBuffer(context_a, 32);"
          }
          else {
            _fpgaOutputTableName = tbl_name_fpga_table
            intermediate_num_cols = 16 // gqe-outputs a 16-col tbl
            outputTblCol += "Table " + _fpgaOutputTableName + "(\"" + _fpgaOutputTableName + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
            if (sf == 30) {
              outputTblCol += _fpgaOutputTableName + ".allocateHost(1.2, hpTimes_aggr);"
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_a, 33);"
              part_tbl_array_name = tbl_name + "_partition_array"
              _fpgaOutputDevAllocateCode += "Table " + part_tbl_array_name + "[hpTimes_aggr];"
              _fpgaOutputDevAllocateCode += "for (int i(0); i < hpTimes_aggr; ++i) {"
              _fpgaOutputDevAllocateCode += "    " + part_tbl_array_name + "[i] = " + _fpgaOutputTableName + ".createSubTable(i);"
              _fpgaOutputDevAllocateCode += "}"
              // add another output table, to shrink and shuffle 16-col table to an 8-col table, when using the gqe-aggr overlay
              outputTblCol += "Table " + tbl_name + "(\"" + tbl_name + "\", " + max_num_rows + ", " + _outputCols.length + ", \"\");"
            } else {
              outputTblCol += _fpgaOutputTableName + ".allocateHost();"
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_a, 33);"
              // add another output table, to shrink and shuffle 16-col table to an 8-col table, when using the gqe-aggr overlay
              outputTblCol += "Table " + tbl_name + "(\"" + tbl_name + "\", " + max_num_rows + ", " + _outputCols.length + ", \"\");"
              outputTblCol += tbl_name + ".allocateHost();"
            }
          }
        }
        else if (parentNode != null && parentNode.cpuORfpga == 1) {
          _fpgaOutputTableName = tbl_name
          intermediate_num_cols = _outputCols.length
          outputTblCol += "Table " + _fpgaOutputTableName + "(\"" + _fpgaOutputTableName + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
          outputTblCol += _fpgaOutputTableName + ".allocateHost();"
          if (parentNode._fpgaOverlayType == 0) {
            if (sf == 30) {
              part_tbl_name = tbl_name + "_partition"
              part_tbl_array_name = tbl_name + "_partition_array"
              outputTblCol += "Table " + part_tbl_name + "(\"" + part_tbl_name + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
              outputTblCol += part_tbl_name + ".allocateHost(1.2, hpTimes_join);"
              outputTblCol += part_tbl_name + ".allocateDevBuffer(context_h, 32);"
              outputTblCol += "Table " + part_tbl_array_name + "[hpTimes_join];"
              outputTblCol += "for (int i(0); i < hpTimes_join; ++i) {"
              outputTblCol += "    " + part_tbl_array_name + "[i] = " + part_tbl_name + ".createSubTable(i);"
              outputTblCol += "}"
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_h, 33);"
            } else {
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_h, 32);"
            }
          }
          else if (parentNode._fpgaOverlayType == 1) {
            if (sf == 30) {
              part_tbl_name = tbl_name + "_partition"
              part_tbl_array_name = tbl_name + "_partition_array"
              outputTblCol += "Table " + part_tbl_name + "(\"" + part_tbl_name + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
              outputTblCol += part_tbl_name + ".allocateHost(1.2, hpTimes_aggr);"
              outputTblCol += part_tbl_name + ".allocateDevBuffer(context_a, 32);"
              outputTblCol += "Table " + part_tbl_array_name + "[hpTimes_aggr];"
              outputTblCol += "for (int i(0); i < hpTimes_aggr; ++i) {"
              outputTblCol += "    " + part_tbl_array_name + "[i] = " + part_tbl_name + ".createSubTable(i);"
              outputTblCol += "}"
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_a, 33);"
            } else {
              _fpgaOutputDevAllocateCode += _fpgaOutputTableName + ".allocateDevBuffer(context_a, 32);"
            }
          }
        }
        else {
          _fpgaOutputTableName = tbl_name
          intermediate_num_cols = _outputCols.length
          outputTblCol += "Table " + _fpgaOutputTableName + "(\"" + _fpgaOutputTableName + "\", " + max_num_rows + ", " + intermediate_num_cols + ", \"\");"
          outputTblCol += _fpgaOutputTableName + ".allocateHost();"
        }

        _fpgaOutputCode ++= outputTblCol
        _fpgaCode ++= outputTblCol
      }
    }
    //-----------------------------------PRE-PROCESSING-----------------------------------------------
    if (_nodeType == "Aggregate" || _nodeType == "Project") {
      for (groupBy_payload <- _aggregate_expression) {
        var col_symbol = groupBy_payload.toString.split(" AS ").last
        var col_type = groupBy_payload.dataType.toString
        columnDictionary += (col_symbol -> (col_type, "NULL"))
      }
    }
    //-----------------------------------OPERATIONS---------------------------------------------------
    if (_operation.nonEmpty) {
      if (_cpuORfpga == 1) { // Execute on FPGA - generate L2 module configurations
        var joinTblOrderSwapped = false
        if (_fpgaOverlayType == 0) { // gqe_join-0, gqe_aggr-1, gqe_part-2
          // tag:overlay0
          var nodeCfgCmd = "cfg_" + nodeOpName + "_cmds"
          var nodeGetCfgFuncName = "get_cfg_dat_" + nodeOpName + "_gqe_join"
          _fpgaConfigCode += "cfgCmd " + nodeCfgCmd + ";"
          _fpgaConfigCode += nodeCfgCmd + ".allocateHost();"
          _fpgaConfigCode += nodeGetCfgFuncName + " (" + nodeCfgCmd + ".cmd);"
          _fpgaConfigCode += nodeCfgCmd + ".allocateDevBuffer(context_h, 32);"

          // Generate the function that generates the Xilinx L2 module configurations
          var joinConcatenateFuncCode = new ListBuffer[String]()
          var cfgFuncCode = new ListBuffer[String]()
          cfgFuncCode += "void " + nodeGetCfgFuncName + "(ap_uint<512>* hbuf) {"
          var cfgFuncCode_gqePart = new ListBuffer[String]()
          if (sf == 30) {
            cfgFuncCode_gqePart += "void " + nodeGetCfgFuncName + "_part" + "(ap_uint<512>* hbuf) {"
          }
          //    find joining terms (if there is any)
          var join_operator = getJoinOperator(this)
          var leftmost_operator = getLeftMostBindedOperator(this)
          var rightmost_operator = getRightMostBindedOperator(this)
          var join_left_table_col = leftmost_operator._children.head.outputCols
          var join_right_table_col = rightmost_operator._children.last.outputCols
          var join_left_table_name = leftmost_operator._children.head._fpgaOutputTableName
          var join_right_table_name = rightmost_operator._children.last._fpgaOutputTableName

          if (leftmost_operator == rightmost_operator && join_left_table_col == join_right_table_col) {
            //non-join operators - no right table
            join_right_table_col = new ListBuffer[String]()
            rightmost_operator = null
          }
          else {
            //join operators
            var largerLeftTbl = false
            if (leftmost_operator._children.head._nodeType == "SerializeFromObject" && rightmost_operator._children.last._nodeType == "SerializeFromObject") {
              if (leftmost_operator._children.head._numTableRow > rightmost_operator._children.last._numTableRow) {
                largerLeftTbl = true
              }
            }
            //              // table order swapping based on number of rows to reduce hashmap creation size
            //              if (join_operator._nodeType == "JOIN_INNER") {
            //                if ((leftmost_operator._children.head._nodeType == "SerializeFromObject" && leftmost_operator._children.head._numTableRow >= 150000 && rightmost_operator._children.last._nodeType != "SerializeFromObject") ||
            //                  (leftmost_operator._children.head._nodeType.contains("JOIN") && rightmost_operator._children.last._nodeType == "Filter") ||
            //                  (rightmost_operator._children.last._nodeType == "SerializeFromObject" && rightmost_operator._children.last._numTableRow < 150000) ||
            //                  largerLeftTbl) {
            //                  var temp_operator = rightmost_operator
            //                  rightmost_operator = leftmost_operator
            //                  leftmost_operator = temp_operator
            //                  join_left_table_col = leftmost_operator._children.last.outputCols
            //                  join_right_table_col = rightmost_operator._children.head.outputCols
            //                  join_left_table_name = leftmost_operator._children.last._fpgaOutputTableName
            //                  join_right_table_name = rightmost_operator._children.head._fpgaOutputTableName
            //                }
            //              }

            // Alec hack - tag:inner_join_order
            // if ( // 632
            if ((queryNum == 1 && (join_operator._treeDepth == 4 || join_operator._treeDepth == 3 || join_operator._treeDepth == 2) && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 2 && (join_operator._treeDepth == 5 || join_operator._treeDepth == 4) && join_operator._nodeType == "JOIN_INNER") ||
              // (queryNum == 2 && join_operator._treeDepth == 7 && join_operator._nodeType == "JOIN_INNER") || // 297
              // if ((queryNum == 2 && join_operator._treeDepth == 8 && join_operator._nodeType == "JOIN_INNER") || // 415
              // if ((queryNum == 2 && join_operator._treeDepth == 9 && join_operator._nodeType == "JOIN_INNER") || // 558
              // if ((queryNum == 2 && join_operator._treeDepth == 7 && join_operator._treeDepth == 8 && join_operator._treeDepth == 9 && join_operator._nodeType == "JOIN_INNER") || //632
              (queryNum == 3 && join_operator._treeDepth == 2 && join_operator._nodeType == "JOIN_INNER") || // 255, without is better => 103 ms
              (queryNum == 5 && (join_operator._treeDepth == 2 || join_operator._treeDepth == 3 || join_operator._treeDepth == 4) && join_operator._nodeType == "JOIN_INNER") || //TPCH
              // (queryNum == 5 && (join_operator._treeDepth == 8) && join_operator._nodeType == "JOIN_INNER") || // TPCDS
              (queryNum == 4 && (join_operator._treeDepth == 5 || join_operator._treeDepth == 3) && join_operator._nodeType == "JOIN_INNER") || // TPCDS
              (queryNum == 6 && (join_operator._treeDepth == 5) && join_operator._nodeType == "JOIN_INNER") || // TPCDS
              (queryNum == 7 && (join_operator._treeDepth == 3 || join_operator._treeDepth == 4 || join_operator._treeDepth == 6) && join_operator._nodeType == "JOIN_INNER") ||
              // (queryNum == 7 && (join_operator._treeDepth == 3 || join_operator._treeDepth == 4 || join_operator._treeDepth == 5 || join_operator._treeDepth == 6) && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 8 && (join_operator._treeDepth == 3 || join_operator._treeDepth == 4 || join_operator._treeDepth == 5 || join_operator._treeDepth == 8) && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 9 && (join_operator._treeDepth == 3 || join_operator._treeDepth == 6) && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 10 && (join_operator._treeDepth == 2 || join_operator._treeDepth == 3 || join_operator._treeDepth == 4) && join_operator._nodeType == "JOIN_INNER") ||
              //                 (queryNum == 11 && join_operator._treeDepth == 4 && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 11 && (join_operator._treeDepth == 3 || join_operator._treeDepth == 4) && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 12 && join_operator._treeDepth == 2 && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 14 && join_operator._treeDepth == 1 && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 16 && join_operator._treeDepth == 2 && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 17 && join_operator._treeDepth == 2 && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 21 && join_operator._treeDepth == 2 && join_operator._nodeType == "JOIN_INNER") ||
              (queryNum == 23 && join_operator._treeDepth == 0 && join_operator._nodeType == "JOIN_INNER"))
            {
              joinTblOrderSwapped = true
              var temp_operator = rightmost_operator
              rightmost_operator = leftmost_operator
              leftmost_operator = temp_operator
              join_left_table_col = leftmost_operator._children.last.outputCols
              join_right_table_col = rightmost_operator._children.head.outputCols
              join_left_table_name = leftmost_operator._children.last._fpgaOutputTableName
              join_right_table_name = rightmost_operator._children.head._fpgaOutputTableName
            }

            //////// Alec-added: code section below is correct //////////////////////////////
            if (join_operator._nodeType == "JOIN_LEFTSEMI" || (join_operator._nodeType == "JOIN_LEFTANTI" && join_operator._operation.head.contains("OR isnull")) || (join_operator._nodeType == "JOIN_LEFTANTI")) {
              joinTblOrderSwapped = true
              var temp_operator = rightmost_operator
              rightmost_operator = leftmost_operator
              leftmost_operator = temp_operator
              join_left_table_col = leftmost_operator._children.last.outputCols
              join_right_table_col = rightmost_operator._children.head.outputCols
              join_left_table_name = leftmost_operator._children.last._fpgaOutputTableName
              join_right_table_name = rightmost_operator._children.head._fpgaOutputTableName
            }
            /////////////////////////////////////////////////////////////////////////////////
          }

          var nodeCfgCmd_part = "cfg_" + nodeOpName + "_cmds_part"
          if (sf == 30) {
            if ((leftmost_operator._children.last._cpuORfpga == 0 || leftmost_operator._children.last._nodeType == "SerializeFromObject") ||
              (rightmost_operator._children.head._cpuORfpga == 0 || rightmost_operator._children.head._nodeType == "SerializeFromObject")){
              _fpgaConfigCode += "cfgCmd " + nodeCfgCmd_part + ";"
              _fpgaConfigCode += nodeCfgCmd_part + ".allocateHost();"
              _fpgaConfigCode += nodeGetCfgFuncName + "_part" + " (" + nodeCfgCmd_part + ".cmd);"
              _fpgaConfigCode += nodeCfgCmd_part + ".allocateDevBuffer(context_h, 32);"
            }
          }

          // ----------------------------------- Debug info -----------------------------------
          cfgFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
          cfgFuncCode += "    // Supported operation: " + _nodeType
          cfgFuncCode += "    // Operation: " + _operation
          for (binded_overlay <- _bindedOverlayInstances) {
            cfgFuncCode += "        // Binded Operation: " + binded_overlay._nodeType + " -> operations: " + binded_overlay._operation
          }
          for (binded_overlay <- _bindedOverlayInstances_left) {
            cfgFuncCode += "        // Binded Operation: " + binded_overlay._nodeType + " -> operations: " + binded_overlay._operation
          }
          for (binded_overlay <- _bindedOverlayInstances_right) {
            cfgFuncCode += "        // Binded Operation: " + binded_overlay._nodeType + " -> operations: " + binded_overlay._operation
          }
          cfgFuncCode += "    // Left Table: " + join_left_table_col
          cfgFuncCode += "    // Right Table: " + join_right_table_col
          cfgFuncCode += "    // Output Table: " + _outputCols
          cfgFuncCode += "    // Node Depth: " + _treeDepth
          cfgFuncCode += "    ap_uint<512>* b = hbuf;"
          cfgFuncCode += "    memset(b, 0, sizeof(ap_uint<512>) * 9);"
          cfgFuncCode += "    ap_uint<512> t = 0;"
          if (sf == 30) {
            cfgFuncCode_gqePart += "    ap_uint<512>* b = hbuf;"
            cfgFuncCode_gqePart += "    memset(b, 0, sizeof(ap_uint<512>) * 9);"
            cfgFuncCode_gqePart += "    ap_uint<512> t = 0;"
            cfgFuncCode_gqePart += ""
          }

          // DRAM -> filter - input readin
          //    A record to track how inputs cols navigates through the overlay
          // table a
          var a_col_idx_dict_prev = collection.mutable.Map[String, Int]()
          var a_idx_col_dict_prev = collection.mutable.Map[Int, String]()
          var a_tmp_idx = 0
          for (i_col <- join_left_table_col) {
            a_col_idx_dict_prev += (i_col -> a_tmp_idx)
            a_idx_col_dict_prev += (a_tmp_idx -> i_col)
            a_tmp_idx += 1
          }
          // table b
          var b_col_idx_dict_prev = collection.mutable.Map[String, Int]()
          var b_idx_col_dict_prev = collection.mutable.Map[Int, String]()
          var b_tmp_idx = 0
          for (i_col <- join_right_table_col) {
            b_col_idx_dict_prev += (i_col -> b_tmp_idx)
            b_idx_col_dict_prev += (b_tmp_idx -> i_col)
            b_tmp_idx += 1
          }

          // filter -> join
          cfgFuncCode += ""
          cfgFuncCode += "    //--------------filter--------------"
          //--------find left table filtering (if there is any)
          var a_col_idx_dict_next = a_col_idx_dict_prev.clone()
          var a_idx_col_dict_next = a_idx_col_dict_prev.clone()
          var filter_a_config_call = "gen_pass_fcfg";
          a_tmp_idx = 0
          if (leftmost_operator != null && leftmost_operator._filtering_expression != null) {
            // move filter operation reference cols to front of the table
            for (ref_col <- leftmost_operator._filtering_expression.references) {
              var prev_ref_idx = a_col_idx_dict_next(ref_col.toString)
              var tmp_col = a_idx_col_dict_next(a_tmp_idx)
              a_col_idx_dict_next(tmp_col) = prev_ref_idx
              a_idx_col_dict_next(prev_ref_idx) = tmp_col
              a_col_idx_dict_next(ref_col.toString) = a_tmp_idx
              a_idx_col_dict_next(a_tmp_idx) = ref_col.toString
              a_tmp_idx += 1
            }
            // filter config call
            var filterCfgFuncName = "gen_fcfg_" + _fpgaNodeName + "_tbl_a"
            filter_a_config_call = filterCfgFuncName
            var filterConfigFuncCode = getFilterConfigFuncCode(filterCfgFuncName, leftmost_operator, a_col_idx_dict_next)
            for (line <- filterConfigFuncCode) {
              _fpgaConfigFuncCode += line
            }
          }
          cfgFuncCode += "    // input table a"
          var tbl_A_col_list = "signed char id_a[] = {"
          var num_tbl_A_col = a_col_idx_dict_prev.size
          for (a <- 0 to 8 - 1) {
            if (a < num_tbl_A_col) { // input cols
              // see Q4 for verification
              var col_name = a_idx_col_dict_next(a)
              var prev_col_idx = a_col_idx_dict_prev(col_name)
              //                var col_name = a_idx_col_dict_prev(a)
              //                var prev_col_idx = a_col_idx_dict_next(col_name)
              tbl_A_col_list += prev_col_idx.toString + ","
            }
            else {
              tbl_A_col_list += "-1,"
            }
          }
          tbl_A_col_list = tbl_A_col_list.stripSuffix(",")
          tbl_A_col_list += "};"
          cfgFuncCode += "    " + tbl_A_col_list
          cfgFuncCode += "    for (int c = 0; c < 8; ++c) {"
          cfgFuncCode += "        t.range(56 + 8 * c + 7, 56 + 8 * c) = id_a[c];"
          cfgFuncCode += "    }"
          cfgFuncCode += "    // filter tbl_a config"
          cfgFuncCode += "    uint32_t cfga[45];"
          cfgFuncCode += "    " + filter_a_config_call + "(cfga);"
          cfgFuncCode += "    memcpy(&b[3], cfga, sizeof(uint32_t) * 45);"

          //--------find right table filtering (if there is any)
          var b_col_idx_dict_next = b_col_idx_dict_prev.clone()
          var b_idx_col_dict_next = b_idx_col_dict_prev.clone()
          b_tmp_idx = 0
          var filter_b_config_call = "gen_pass_fcfg";
          if (rightmost_operator != null && rightmost_operator._filtering_expression != null) {
            // TODO: move filter operation reference cols to front of the table
            for (ref_col <- rightmost_operator._filtering_expression.references) {
              var prev_ref_idx = b_col_idx_dict_next(ref_col.toString)
              var tmp_col = b_idx_col_dict_next(b_tmp_idx)
              b_col_idx_dict_next(tmp_col) = prev_ref_idx
              b_idx_col_dict_next(prev_ref_idx) = tmp_col
              b_col_idx_dict_next(ref_col.toString) = b_tmp_idx
              b_idx_col_dict_next(b_tmp_idx) = ref_col.toString
              b_tmp_idx += 1
            }
            // filter config call
            var filterCfgFuncName = "gen_fcfg_" + _fpgaNodeName + "_tbl_b"
            filter_b_config_call = filterCfgFuncName
            var filterConfigFuncCode = getFilterConfigFuncCode(filterCfgFuncName, rightmost_operator, b_col_idx_dict_next)
            for (line <- filterConfigFuncCode) {
              _fpgaConfigFuncCode += line
            }
          }
          cfgFuncCode += ""
          cfgFuncCode += "    // input table b"
          var tbl_B_col_list = "signed char id_b[] = {"
          var num_tbl_B_col = b_col_idx_dict_prev.size
          for (b <- 0 to 8 - 1) {
            if (b < num_tbl_B_col) { // input cols
              // see Q4 for verification
              var col_name = b_idx_col_dict_next(b)
              var prev_col_idx = b_col_idx_dict_prev(col_name)
              //                var col_name = b_idx_col_dict_prev(b)
              //                var prev_col_idx = b_col_idx_dict_next(col_name)
              tbl_B_col_list += prev_col_idx.toString + ","
            }
            else {
              tbl_B_col_list += "-1,"
            }
          }
          tbl_B_col_list = tbl_B_col_list.stripSuffix(",")
          tbl_B_col_list += "};"
          cfgFuncCode += "    " + tbl_B_col_list
          cfgFuncCode += "    for (int c = 0; c < 8; ++c) {"
          cfgFuncCode += "        t.range(120 + 8 * c + 7, 120 + 8 * c) = id_b[c];"
          cfgFuncCode += "    }"
          cfgFuncCode += "    // filter tbl_b config"
          cfgFuncCode += "    uint32_t cfgb[45];"
          cfgFuncCode += "    " + filter_b_config_call + "(cfgb);"
          cfgFuncCode += "    memcpy(&b[6], cfgb, sizeof(uint32_t) * 45);"
          // update prev with next
          a_col_idx_dict_prev = a_col_idx_dict_next.clone()
          a_idx_col_dict_prev = a_idx_col_dict_next.clone()
          b_col_idx_dict_prev = b_col_idx_dict_next.clone()
          b_idx_col_dict_prev = b_idx_col_dict_next.clone()

          // join -> eval
          cfgFuncCode += ""
          cfgFuncCode += "    //--------------join--------------"
          var join_on = 0
          var dual_key_join_on = 0
          var join_flag = 0
          var leftTableColShuffleIdx = new ListBuffer[Int]()
          var rightTableColShuffleIdx = new ListBuffer[Int]()
          var leftTableKeyColShuffleName = new ListBuffer[String]()
          var rightTableKeyColShuffleName = new ListBuffer[String]()
          var leftTablePayloadColShuffleName = new ListBuffer[String]()
          var rightTablePayloadColShuffleName = new ListBuffer[String]()
          if (join_operator != null) { // valid join op
            join_on = 1
            // join dual key
            var join_key_pairs = getJoinKeyTerms(join_operator._joining_expression(0), false)
            var num_join_key_pairs = join_key_pairs.length
            if (num_join_key_pairs > 2) {
              cfgFuncCode += "    Unsupported multi-key pairs more than two"
              dual_key_join_on = -1
            }
            else if (num_join_key_pairs == 2 && !join_operator._isSpecialSemiJoin) {
              dual_key_join_on = 1
            }
            else {
              dual_key_join_on = 0
            }
            // join type
            join_operator._nodeType match {
              case "JOIN_INNER" =>
                join_flag = 0
              case "JOIN_LEFTSEMI" =>
                if (join_operator._isSpecialSemiJoin) {
                  join_flag = 3
                }
                else {
                  join_flag = 1
                }
              case "JOIN_LEFTANTI" =>
                join_flag = 2
              case _ =>
                join_flag = -1
            }

            // updating output_alise col with output col
            if (leftmost_operator._outputCols_alias.nonEmpty) {
              var this_idx = 0
              for (o_col_alias <- leftmost_operator._outputCols) {
                var curr_col = leftmost_operator._outputCols_alias(this_idx)
                var curr_idx = a_col_idx_dict_prev(curr_col)
                a_col_idx_dict_prev.remove(curr_col)
                a_idx_col_dict_prev.remove(curr_idx)
                a_col_idx_dict_prev += (o_col_alias -> curr_idx)
                a_idx_col_dict_prev += (curr_idx -> o_col_alias)
                this_idx += 1
              }
            }
            if (rightmost_operator._outputCols_alias.nonEmpty) {
              var this_idx = 0
              for (o_col_alias <- rightmost_operator._outputCols) {
                var curr_col = rightmost_operator._outputCols_alias(this_idx)
                var curr_idx = b_col_idx_dict_prev(curr_col)
                b_col_idx_dict_prev.remove(curr_col)
                b_idx_col_dict_prev.remove(curr_idx)
                b_col_idx_dict_prev += (o_col_alias -> curr_idx)
                b_idx_col_dict_prev += (curr_idx -> o_col_alias)
                this_idx += 1
              }
            }

            // join key-payload col rearrangement
            for (key_pair <- join_key_pairs) {
              var leftTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" = ").head
              var rightTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" = ").last
              if (join_operator._isSpecialSemiJoin && key_pair.contains(" != ")) {
                leftTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" != ").head
                rightTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" != ").last
              }
              else if (join_operator._isSpecialAntiJoin && key_pair.contains(" != ")) {
                leftTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" != ").head
                rightTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" != ").last
              }

              var needTableColSwap = false
              if (a_col_idx_dict_prev.contains(rightTblCol) && b_col_idx_dict_prev.contains(leftTblCol)) { //no alias
                needTableColSwap = true
              }
              if (needTableColSwap) {
                var tmpTblCol = leftTblCol
                leftTblCol = rightTblCol
                rightTblCol = tmpTblCol
              }

              if (join_operator._isSpecialSemiJoin == true && key_pair.contains(" != ")) {
                leftTableColShuffleIdx += a_col_idx_dict_prev(leftTblCol)
                rightTableColShuffleIdx += b_col_idx_dict_prev(rightTblCol)
              } else {
                leftTableColShuffleIdx += a_col_idx_dict_prev(leftTblCol)
                rightTableColShuffleIdx += b_col_idx_dict_prev(rightTblCol)
                leftTableKeyColShuffleName += leftTblCol
                rightTableKeyColShuffleName += rightTblCol
              }
            }
            for (col_idx <- a_col_idx_dict_prev) {
              var col = col_idx._1
              var idx = col_idx._2
              if (leftTableKeyColShuffleName.indexOf(col) == -1) {
                leftTablePayloadColShuffleName += col
                if (!leftTableColShuffleIdx.contains(idx)) {
                  leftTableColShuffleIdx += idx
                }
              }
            }
            for (col_idx <- b_col_idx_dict_prev) {
              var col = col_idx._1
              var idx = col_idx._2
              if (rightTableKeyColShuffleName.indexOf(col) == -1) {
                rightTablePayloadColShuffleName += col
                if (!rightTableColShuffleIdx.contains(idx)) {
                  rightTableColShuffleIdx += idx
                }
              }
            }
          }
          else {
            var idx = 0
            if (this._nodeType == "Filter") {
              //shuffle filtering output cols here
              var temp_col_idx_dict_prev = collection.mutable.Map[String, Int]()
              var temp_idx_col_dict_prev = collection.mutable.Map[Int, String]()
              for (filter_o_col <- this._outputCols) {
                if (a_col_idx_dict_prev.contains(filter_o_col)) {
                  leftTableColShuffleIdx += a_col_idx_dict_prev(filter_o_col)
                } else {
                  var alias_col = this._outputCols_alias(idx)
                  leftTableColShuffleIdx += a_col_idx_dict_prev(alias_col)
                }
                temp_col_idx_dict_prev += (filter_o_col -> idx)
                temp_idx_col_dict_prev += (idx -> filter_o_col)
                idx += 1
              }
              a_col_idx_dict_prev = temp_col_idx_dict_prev
              a_idx_col_dict_prev = temp_idx_col_dict_prev
            }
            else if (this._nodeType == "Aggregate") {
              //shuffle aggregation reference cols here
              var temp_col_idx_dict_prev = collection.mutable.Map[String, Int]()
              var temp_idx_col_dict_prev = collection.mutable.Map[Int, String]()
              // no groupby operation here - gqe-join
              for (this_aggr_expr <- this._aggregate_expression) {
                for (aggregate_input_col <- this_aggr_expr.references) {
                  var aggr_i_col_str = aggregate_input_col.toString
                  leftTableColShuffleIdx += a_col_idx_dict_prev(aggr_i_col_str)
                  temp_col_idx_dict_prev += (aggr_i_col_str -> idx)
                  temp_idx_col_dict_prev += (idx -> aggr_i_col_str)
                  idx += 1
                }
              }
              a_col_idx_dict_prev = temp_col_idx_dict_prev
              a_idx_col_dict_prev = temp_idx_col_dict_prev
            }
            else {
              for ( a_col <- a_col_idx_dict_prev) {
                leftTableColShuffleIdx += idx
                idx += 1
              }
            }
            idx = 0
            for ( b_col <- b_col_idx_dict_prev) {
              rightTableColShuffleIdx += idx
              idx += 1
            }
          }

          if (sf == 30) {
            //gqePart configuration bits
            var filter_a_config_call = "gen_pass_fcfg";
            cfgFuncCode_gqePart += "    // input table a"
            var tbl_A_col_list = "signed char id_a[] = {"
            var num_tbl_A_col = a_col_idx_dict_prev.size
            for (a <- 0 to 8 - 1) {
              if (a < leftTableColShuffleIdx.length) {
                tbl_A_col_list += leftTableColShuffleIdx(a) + ","
              }
              else {
                tbl_A_col_list += "-1,"
              }
            }
            tbl_A_col_list = tbl_A_col_list.stripSuffix(",")
            tbl_A_col_list += "};"
            cfgFuncCode_gqePart += "    " + tbl_A_col_list
            cfgFuncCode_gqePart += "    for (int c = 0; c < 8; ++c) {"
            cfgFuncCode_gqePart += "        t.range(56 + 8 * c + 7, 56 + 8 * c) = id_a[c];"
            cfgFuncCode_gqePart += "    }"
            cfgFuncCode_gqePart += "    // filter tbl_a config"
            cfgFuncCode_gqePart += "    uint32_t cfga[45];"
            cfgFuncCode_gqePart += "    " + filter_a_config_call + "(cfga);"
            cfgFuncCode_gqePart += "    memcpy(&b[3], cfga, sizeof(uint32_t) * 45);"
            var filter_b_config_call = "gen_pass_fcfg";
            cfgFuncCode_gqePart += ""
            cfgFuncCode_gqePart += "    // input table b"
            var tbl_B_col_list = "signed char id_b[] = {"
            var num_tbl_B_col = b_col_idx_dict_prev.size
            for (b <- 0 to 8 - 1) {
              if (b < rightTableColShuffleIdx.length) {
                tbl_B_col_list += rightTableColShuffleIdx(b) + ","
              }
              else {
                tbl_B_col_list += "-1,"
              }
            }
            tbl_B_col_list = tbl_B_col_list.stripSuffix(",")
            tbl_B_col_list += "};"
            cfgFuncCode_gqePart += "    " + tbl_B_col_list
            cfgFuncCode_gqePart += "    for (int c = 0; c < 8; ++c) {"
            cfgFuncCode_gqePart += "        t.range(120 + 8 * c + 7, 120 + 8 * c) = id_b[c];"
            cfgFuncCode_gqePart += "    }"
            cfgFuncCode_gqePart += "    // filter tbl_b config"
            cfgFuncCode_gqePart += "    uint32_t cfgb[45];"
            cfgFuncCode_gqePart += "    " + filter_b_config_call + "(cfgb);"
            cfgFuncCode_gqePart += "    memcpy(&b[6], cfgb, sizeof(uint32_t) * 45);"
            cfgFuncCode_gqePart += ""
            cfgFuncCode_gqePart += "    // join config"
            cfgFuncCode_gqePart += "    t.set_bit(0, " + join_on + ");    // join"
            cfgFuncCode_gqePart += "    t.set_bit(2, " + dual_key_join_on + ");    // dual-key"
            cfgFuncCode_gqePart += "    t.range(5, 3) = " + join_flag + ";  // hash join flag = 0 for normal, 1 for semi, 2 for anti"
            cfgFuncCode_gqePart += ""
            cfgFuncCode_gqePart += "    b[0] = t;"
            cfgFuncCode_gqePart += "}"

            //gqeJoin configuration bits
            cfgFuncCode += "    //stream shuffle 1a"
            cfgFuncCode += "    ap_int<64> shuffle1a_cfg;"
            for (ss1a <- 0 to 8 - 1) {
              if (ss1a < leftTableColShuffleIdx.length) {
                cfgFuncCode += "    shuffle1a_cfg(" + ((ss1a + 1) * 8 - 1).toString + ", " + (ss1a * 8).toString + ") = " + ss1a + ";"
              } else {
                cfgFuncCode += "    shuffle1a_cfg(" + ((ss1a + 1) * 8 - 1).toString + ", " + (ss1a * 8).toString + ") = -1;"
              }
            }
            cfgFuncCode += "\n" + "    //stream shuffle 1b"
            cfgFuncCode += "    ap_int<64> shuffle1b_cfg;"
            for (ss1b <- 0 to 8 - 1) {
              if (ss1b < rightTableColShuffleIdx.length) {
                cfgFuncCode += "    shuffle1b_cfg(" + ((ss1b + 1) * 8 - 1).toString + ", " + (ss1b * 8).toString + ") = " + ss1b + ";"
              } else {
                cfgFuncCode += "    shuffle1b_cfg(" + ((ss1b + 1) * 8 - 1).toString + ", " + (ss1b * 8).toString + ") = -1;"
              }
            }
            cfgFuncCode += ""
            cfgFuncCode += "    // join config"
            cfgFuncCode += "    t.set_bit(0, " + join_on + ");    // join"
            cfgFuncCode += "    t.set_bit(2, " + dual_key_join_on + ");    // dual-key"
            cfgFuncCode += "    t.range(5, 3) = " + join_flag + ";  // hash join flag = 0 for normal, 1 for semi, 2 for anti"
          }
          else {
            cfgFuncCode += "    //stream shuffle 1a"
            cfgFuncCode += "    ap_int<64> shuffle1a_cfg;"
            for (ss1a <- 0 to 8 - 1) {
              if (ss1a < leftTableColShuffleIdx.length) {
                cfgFuncCode += "    shuffle1a_cfg(" + ((ss1a + 1) * 8 - 1).toString + ", " + (ss1a * 8).toString + ") = " + leftTableColShuffleIdx(ss1a) + ";"
              } else {
                cfgFuncCode += "    shuffle1a_cfg(" + ((ss1a + 1) * 8 - 1).toString + ", " + (ss1a * 8).toString + ") = -1;"
              }
            }
            cfgFuncCode += "\n" + "    //stream shuffle 1b"
            cfgFuncCode += "    ap_int<64> shuffle1b_cfg;"
            for (ss1b <- 0 to 8 - 1) {
              if (ss1b < rightTableColShuffleIdx.length) {
                cfgFuncCode += "    shuffle1b_cfg(" + ((ss1b + 1) * 8 - 1).toString + ", " + (ss1b * 8).toString + ") = " + rightTableColShuffleIdx(ss1b) + ";"
              } else {
                cfgFuncCode += "    shuffle1b_cfg(" + ((ss1b + 1) * 8 - 1).toString + ", " + (ss1b * 8).toString + ") = -1;"
              }
            }
            cfgFuncCode += ""
            cfgFuncCode += "    // join config"
            cfgFuncCode += "    t.set_bit(0, " + join_on + ");    // join"
            cfgFuncCode += "    t.set_bit(2, " + dual_key_join_on + ");    // dual-key"
            cfgFuncCode += "    t.range(5, 3) = " + join_flag + ";  // hash join flag = 0 for normal, 1 for semi, 2 for anti"
          }
          var col_idx_dict_prev = collection.mutable.Map[String, Int]()
          var idx_col_dict_prev = collection.mutable.Map[Int, String]()
          if (join_operator != null) {
            var join_output_cols = join_operator._outputCols
            var outputTableColShuffleIdx = new ListBuffer[Int]()
            var outputTableColShuffleName = new ListBuffer[String]()
            if (join_output_cols.length > 8) {
              cfgFuncCode += "    Unsupported number of output columns more than eight"
            }
            for (col <- join_output_cols) {
              if (rightTablePayloadColShuffleName.indexOf(col) != -1) {
                // outputTableColShuffleIdx += rightTablePayloadColShuffleName.indexOf(col)
                col_idx_dict_prev += (col -> rightTablePayloadColShuffleName.indexOf(col))
                idx_col_dict_prev += (rightTablePayloadColShuffleName.indexOf(col) -> col)
                //                  col_idx_dict_prev += (col -> (b_col_idx_dict_prev(col) - 1))
                //                  idx_col_dict_prev += ((b_col_idx_dict_prev(col) - 1) -> col)
              }
              else if (leftTablePayloadColShuffleName.indexOf(col) != -1) {
                // outputTableColShuffleIdx += (leftTablePayloadColShuffleName.indexOf(col) + 6) // +6 because left table col
                col_idx_dict_prev += (col -> (leftTablePayloadColShuffleName.indexOf(col) + 6))
                idx_col_dict_prev += ((leftTablePayloadColShuffleName.indexOf(col) + 6) -> col)
                //                  col_idx_dict_prev += (col -> (a_col_idx_dict_prev(col) - 1 + 6))
                //                  idx_col_dict_prev += ((a_col_idx_dict_prev(col) - 1 + 6) -> col)
              }
              else if (leftTableKeyColShuffleName.indexOf(col) != -1) {
                // outputTableColShuffleIdx += (leftTableKeyColShuffleName.indexOf(col) + 12)
                col_idx_dict_prev += (col -> (leftTableKeyColShuffleName.indexOf(col) + 12))
                idx_col_dict_prev += ((leftTableKeyColShuffleName.indexOf(col) + 12) -> col)
                //                  col_idx_dict_prev += (col -> (a_col_idx_dict_prev(col) - 1 + 12))
                //                  idx_col_dict_prev += ((a_col_idx_dict_prev(col) - 1 + 12) -> col)
              }
              else if (rightTableKeyColShuffleName.indexOf(col) != -1) {
                // outputTableColShuffleIdx += (rightTableKeyColShuffleName.indexOf(col) + 12)
                col_idx_dict_prev += (col -> (rightTableKeyColShuffleName.indexOf(col) + 12))
                idx_col_dict_prev += ((rightTableKeyColShuffleName.indexOf(col) + 12) -> col)
                //                  col_idx_dict_prev += (col -> (b_col_idx_dict_prev(col) - 1 + 12))
                //                  idx_col_dict_prev += ((b_col_idx_dict_prev(col) - 1 + 12) -> col)
              }
            }
          }
          else {
            if (a_col_idx_dict_prev.nonEmpty) {
              col_idx_dict_prev = a_col_idx_dict_prev.clone()
              idx_col_dict_prev = a_idx_col_dict_prev.clone()
            }
            else {
              col_idx_dict_prev = b_col_idx_dict_prev.clone()
              idx_col_dict_prev = b_idx_col_dict_prev.clone()
            }
          }

          //eval0
          cfgFuncCode += ""
          cfgFuncCode += "    //--------------eval0--------------"
          //    find eval0 terms (if there is any)
          // Alec-hack
          // var aggr_operator = getNonGroupByAggregateOperator(this)
          var aggr_operator = getNonGroupByAggregateOperator(this)
          var eval0_func = "NOP"
          var eval0_func_call = "// eval0: NOP"
          var col_idx_dict_next = collection.mutable.Map[String, Int]()
          var idx_col_dict_next = collection.mutable.Map[Int, String]()

          if (aggr_operator != null) {
            if (aggr_operator._aggregate_expression.nonEmpty) {
              var aggr_expr = aggr_operator._aggregate_expression(0) //first aggr expr
              var start_idx = 0
              for (this_aggr_expr <- aggr_operator._aggregate_expression) {
                for (i_col <- this_aggr_expr.references) { // equvalent as -> for (i_col <- aggr_operator._inputCols) {
                  col_idx_dict_next += (i_col.toString -> start_idx)
                  idx_col_dict_next += (start_idx -> i_col.toString)
                  start_idx += 1
                }
              }
              //move all referenced cols in aggr expr to the front
              start_idx = 0 //first output col
              for (ref_col <- aggr_expr.references) {
                // place reference col and idx to 'start_idx'
                var prev_idx = col_idx_dict_next(ref_col.toString)
                col_idx_dict_next(ref_col.toString) = start_idx
                var prev_col = idx_col_dict_next(start_idx)
                idx_col_dict_next(start_idx) = ref_col.toString
                // place the previous col and idx at new location
                col_idx_dict_next(prev_col) = prev_idx
                idx_col_dict_next(prev_idx) = prev_col
                // increment on the next front idx
                start_idx += 1
              }
              // eval dynamicALUCompiler function call
              var tmp_aggr_expr = new ListBuffer[Expression]()
              tmp_aggr_expr += aggr_expr
              if (isPureAggrOperation(tmp_aggr_expr)) {
                eval0_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                eval0_func_call = "// eval0: NOP"
              }
              else {
                eval0_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                var aggr_const_i = new Array[Int](4) // default set as zero
                var strm_order_i = 1
                var (aggr_str, aggr_const, strm_order) = getAggregateExpression_ALUOPCompiler_cmd(aggr_expr, aggr_const_i, strm_order_i)
                aggr_str = aggr_str.stripPrefix("(")
                aggr_str = aggr_str.stripSuffix(")")
                var ALUOPCompiler = "xf::database::dynamicALUOPCompiler<uint32_t, uint32_t, uint32_t, uint32_t>(\""
                ALUOPCompiler += aggr_str + "\", "
                ALUOPCompiler += aggr_const(0) + ", " + aggr_const(1) + ", " + aggr_const(2) + ", " + aggr_const(3) + ", "
                ALUOPCompiler += "op_eval_0);"
                eval0_func_call = ALUOPCompiler
              }
              if (aggr_operator._nodeType == "Project") {
                for (o_col <- _outputCols) {
                  if (!col_idx_dict_next.contains(o_col) && col_idx_dict_prev.contains(o_col)) {
                    col_idx_dict_next += (o_col -> start_idx)
                    idx_col_dict_next += (start_idx -> o_col)
                    start_idx += 1
                  }
                }
              }
              // add aliase col to col_idx and idx_col table
              var aliase_col = aggr_expr.toString.split(" AS ").last
              col_idx_dict_next += (aliase_col -> 8)
              idx_col_dict_next += (8 -> aliase_col)
            }
            else {
              cfgFuncCode += "    // NO aggregation operation - eval0"
            }
          }
          else {
            var start_idx = 0
            for (o_col <- _outputCols) {
              col_idx_dict_next += (o_col -> start_idx)
              idx_col_dict_next += (start_idx -> o_col)
              start_idx += 1
            }
          }

          cfgFuncCode += "    //stream shuffle 2"
          cfgFuncCode += "    ap_int<64> shuffle2_cfg;"
          for (ss2 <- 0 to 8-1) { //max 8 cols for input table
            //TODO: fix the line below
            if (aggr_operator != null && ss2 < idx_col_dict_prev.size) { // input cols - yes aggr
              var col_name = idx_col_dict_next(ss2)
              var prev_col_idx = col_idx_dict_prev(col_name)
              cfgFuncCode += "    shuffle2_cfg(" + ((ss2 + 1) * 8 - 1).toString + ", " + (ss2 * 8).toString + ") = " + prev_col_idx.toString + ";"  + " // " + col_name
            }
            else if (aggr_operator == null && ss2 < idx_col_dict_next.size) { // input cols - no aggr
              var col_name = idx_col_dict_next(ss2)
              var prev_col_idx = col_idx_dict_prev(col_name)
              cfgFuncCode += "    shuffle2_cfg(" + ((ss2 + 1) * 8 - 1).toString + ", " + (ss2 * 8).toString + ") = " + prev_col_idx.toString + ";"  + " // " + col_name
            }
            else {
              cfgFuncCode += "    shuffle2_cfg(" + ((ss2 + 1) * 8 - 1).toString + ", " + (ss2 * 8).toString + ") = -1;"
            }
          }
          cfgFuncCode += ""
          cfgFuncCode += "    ap_uint<289> op_eval_0 = 0;" + " // " + eval0_func
          cfgFuncCode += "    " + eval0_func_call
          cfgFuncCode += "    b[1] = op_eval_0;"
          // update prev with next
          col_idx_dict_prev = col_idx_dict_next.clone()
          idx_col_dict_prev = idx_col_dict_next.clone()

          //eval1
          cfgFuncCode += ""
          cfgFuncCode += "    //--------------eval1--------------"
          var eval1_func = "NOP"
          var eval1_func_call = "// eval1: NOP"

          if (aggr_operator != null) {
            if (aggr_operator._aggregate_expression.nonEmpty && aggr_operator._aggregate_expression.length == 2) {
              var aggr_expr = aggr_operator._aggregate_expression(1) //second aggr expr
              //move all referenced cols in aggr expr to the front
              var start_idx = 0
              for (ref_col <- aggr_expr.references) {
                // place reference col and idx to 'start_idx'
                var prev_idx = col_idx_dict_next(ref_col.toString)
                col_idx_dict_next(ref_col.toString) = start_idx
                var prev_col = idx_col_dict_next(start_idx)
                idx_col_dict_next(start_idx) = ref_col.toString
                // place the previous col and idx at new location
                col_idx_dict_next(prev_col) = prev_idx
                idx_col_dict_next(prev_idx) = prev_col
                // increment on the next front idx
                start_idx += 1
              }

              // move eval0 result from 8th col to append after the normal cols
              var eval0_result_col_name = idx_col_dict_next(8)
              col_idx_dict_next(eval0_result_col_name) = idx_col_dict_next.size-1
              idx_col_dict_next.remove(8)
              idx_col_dict_next += (idx_col_dict_next.size -> eval0_result_col_name)

              // eval dynamicALUCompiler function call
              var tmp_aggr_expr = new ListBuffer[Expression]()
              tmp_aggr_expr += aggr_expr
              if (isPureEvalOperation(tmp_aggr_expr)) {
                eval1_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                eval1_func_call = ""
              }
              else {
                eval1_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                var aggr_const_i = new Array[Int](4) // default set as zero
                var strm_order_i = 1
                var (aggr_str, aggr_const, strm_order) = getAggregateExpression_ALUOPCompiler_cmd(aggr_expr, aggr_const_i, strm_order_i)
                aggr_str = aggr_str.stripPrefix("(")
                aggr_str = aggr_str.stripSuffix(")")
                var ALUOPCompiler = "xf::database::dynamicALUOPCompiler<uint32_t, uint32_t, uint32_t, uint32_t>(\""
                ALUOPCompiler += aggr_str + "\", "
                ALUOPCompiler += aggr_const(0) + ", " + aggr_const(1) + ", " + aggr_const(2) + ", " + aggr_const(3) + ", "
                ALUOPCompiler += "op_eval_1);"
                eval1_func_call = ALUOPCompiler
              }

              // add aliase col to col_idx and idx_col table
              var aliase_col = aggr_expr.toString.split(" AS ").last
              col_idx_dict_next += (aliase_col -> 8)
              idx_col_dict_next += (8 -> aliase_col)
            }
            else {
              cfgFuncCode += "    // NO aggregation operation - eval1"

              // if eval0 != null, move eval0 result from 8th col to append after the normal cols
              if (idx_col_dict_next.contains(8)) {
                var eval0_result_col_name = idx_col_dict_next(8)
                col_idx_dict_next(eval0_result_col_name) = idx_col_dict_next.size-1
                idx_col_dict_next.remove(8)
                idx_col_dict_next += (idx_col_dict_next.size -> eval0_result_col_name)
              }
            }
          }

          cfgFuncCode += "    //stream shuffle 3"
          cfgFuncCode += "    ap_int<64> shuffle3_cfg;"
          for (ss3 <- 0 to 8-1) { //max 8 cols for input table
            //TODO: fix the line below
            if (ss3 < idx_col_dict_prev.size) { // normal cols
              if (idx_col_dict_prev.contains(ss3)) {
                var col_name = idx_col_dict_next(ss3)
                var prev_col_idx = col_idx_dict_prev(col_name)
                cfgFuncCode += "    shuffle3_cfg(" + ((ss3 + 1) * 8 - 1).toString + ", " + (ss3 * 8).toString + ") = " + prev_col_idx.toString + ";"  + " // " + col_name
              } else { // eval col - always at idx 8
                cfgFuncCode += "    shuffle3_cfg(" + ((ss3 + 1) * 8 - 1).toString + ", " + (ss3 * 8).toString + ") = " + 8 + ";"  + " // " + idx_col_dict_prev(8)
              }
            }
            else {
              cfgFuncCode += "    shuffle3_cfg(" + ((ss3 + 1) * 8 - 1).toString + ", " + (ss3 * 8).toString + ") = -1;"
            }
          }
          cfgFuncCode += ""
          cfgFuncCode += "    ap_uint<289> op_eval_1 = 0;" + " // " + eval1_func
          cfgFuncCode += "    " + eval1_func_call
          cfgFuncCode += "    b[2] = op_eval_1;"
          // update prev with next
          col_idx_dict_prev = col_idx_dict_next.clone()
          idx_col_dict_prev = idx_col_dict_next.clone()

          // aggr
          cfgFuncCode += ""
          cfgFuncCode += "    //--------------aggregate--------------"
          var aggr_on = 0
          if (aggr_operator != null) {
            if (!isPureEvalOperation(aggr_operator._aggregate_expression)) {
              aggr_on = 1
            }
          }
          // Shuffle aggreation cols for the final write-out cols
          var temp_col_idx_dict_next = collection.mutable.Map[String, Int]()
          var temp_idx_col_dict_next = collection.mutable.Map[Int, String]()
          var idx = 0
          for (o_col <- this._outputCols) {
            temp_col_idx_dict_next += (o_col -> idx)
            temp_idx_col_dict_next += (idx -> o_col)
            idx += 1
          }
          col_idx_dict_next = temp_col_idx_dict_next
          idx_col_dict_next = temp_idx_col_dict_next

          cfgFuncCode += "    //stream shuffle 4"
          cfgFuncCode += "    ap_int<64> shuffle4_cfg;"
          for (ss4 <- 0 to 8-1) { //max 8 cols for input table
            if (ss4 < idx_col_dict_next.size) { // normal cols
              var col_name = idx_col_dict_next(ss4)
              var prev_col_idx = col_idx_dict_prev(col_name)
              cfgFuncCode += "    shuffle4_cfg(" + ((ss4 + 1) * 8 - 1).toString + ", " + (ss4 * 8).toString + ") = " + prev_col_idx.toString + ";"  + " // " + col_name
            }
            else {
              cfgFuncCode += "    shuffle4_cfg(" + ((ss4 + 1) * 8 - 1).toString + ", " + (ss4 * 8).toString + ") = -1;"
            }
          }
          cfgFuncCode += ""
          cfgFuncCode += "    t.set_bit(1, " + aggr_on + "); // aggr flag"
          // update prev with next
          col_idx_dict_prev = col_idx_dict_next.clone()
          idx_col_dict_prev = idx_col_dict_next.clone()

          // writeout
          cfgFuncCode += ""
          cfgFuncCode += "    //--------------writeout--------------"
          // TODO: output col selection
          cfgFuncCode += "    // output table col"
          var tbl_C_col_list = "t.range(191, 184) = {"
          var c = 0
          for (c <- 0 to 8 - 1) {
            if (c < idx_col_dict_prev.size) {
              tbl_C_col_list += pow(2, c).intValue.toString + "*1 + "
            } else {
              tbl_C_col_list += pow(2, c).intValue.toString + "*0 + "
            }
          }
          tbl_C_col_list = tbl_C_col_list.stripSuffix(" + ")
          tbl_C_col_list += "};"
          cfgFuncCode += "    " + tbl_C_col_list
          cfgFuncCode += "    b[0] = t;"

          cfgFuncCode += "\n" + "    //stream shuffle assignment"
          cfgFuncCode += "    b[0].range(255, 192) = shuffle1a_cfg;"
          cfgFuncCode += "    b[0].range(319, 256) = shuffle1b_cfg;"
          cfgFuncCode += "    b[0].range(383, 320) = shuffle2_cfg;"
          cfgFuncCode += "    b[0].range(447, 384) = shuffle3_cfg;"
          cfgFuncCode += "    b[0].range(511, 448) = shuffle4_cfg;"
          cfgFuncCode += "}"
          for (line <- cfgFuncCode) {
            _fpgaConfigFuncCode += line
          }
          if (sf == 30) {
            for (line <- cfgFuncCode_gqePart) {
              _fpgaConfigFuncCode += line
            }
          }
          // TODO: Set up Xilinx L2 module kernels in host code
          // TODO: Set up Xilinx L2 module kernels
          var kernel_name = ""
          var kernel_name_left = ""
          var kernel_name_right = ""
          if (sf == 30) {
            if (joinTblOrderSwapped == false) {
              if (leftmost_operator._children.head._cpuORfpga == 0 || leftmost_operator._children.head._nodeType == "SerializeFromObject") {
                kernel_name_left = "krnl_" + nodeOpName + "_part_left"
                _fpgaKernelSetupCode += "krnlEngine " + kernel_name_left + ";"
                _fpgaKernelSetupCode += kernel_name_left + " = krnlEngine(program_h, q_h, \"gqePart\");"
                _fpgaKernelSetupCode += kernel_name_left + ".setup_hp(512, 0, power_of_hpTimes_join, " + join_left_table_name + ", " + join_left_table_name + "_partition" + ", " + nodeCfgCmd_part +");"
              }
              if (_children.length > 1 && (rightmost_operator._children.last._cpuORfpga == 0 || rightmost_operator._children.last._nodeType == "SerializeFromObject")) {
                kernel_name_right = "krnl_" + nodeOpName + "_part_right"
                _fpgaKernelSetupCode += "krnlEngine " + kernel_name_right + ";"
                _fpgaKernelSetupCode += kernel_name_right + " = krnlEngine(program_h, q_h, \"gqePart\");"
                _fpgaKernelSetupCode += kernel_name_right + ".setup_hp(512, 1, power_of_hpTimes_join, " + join_right_table_name + ", " + join_right_table_name + "_partition" + ", " + nodeCfgCmd_part +");"
              }
            } else {
              if (leftmost_operator._children.last._cpuORfpga == 0 || leftmost_operator._children.last._nodeType == "SerializeFromObject") {
                kernel_name_left = "krnl_" + nodeOpName + "_part_left"
                _fpgaKernelSetupCode += "krnlEngine " + kernel_name_left + ";"
                _fpgaKernelSetupCode += kernel_name_left + " = krnlEngine(program_h, q_h, \"gqePart\");"
                _fpgaKernelSetupCode += kernel_name_left + ".setup_hp(512, 0, power_of_hpTimes_join, " + join_left_table_name + ", " + join_left_table_name + "_partition" + ", " + nodeCfgCmd_part +");"
              }
              if (_children.length > 1 && (rightmost_operator._children.head._cpuORfpga == 0 || rightmost_operator._children.head._nodeType == "SerializeFromObject")) {
                kernel_name_right = "krnl_" + nodeOpName + "_part_right"
                _fpgaKernelSetupCode += "krnlEngine " + kernel_name_right + ";"
                _fpgaKernelSetupCode += kernel_name_right + " = krnlEngine(program_h, q_h, \"gqePart\");"
                _fpgaKernelSetupCode += kernel_name_right + ".setup_hp(512, 1, power_of_hpTimes_join, " + join_right_table_name + ", " + join_right_table_name + "_partition" + ", " + nodeCfgCmd_part +");"
              }
            }

            kernel_name = "krnl_" + nodeOpName
            _fpgaKernelSetupCode += "krnlEngine " + kernel_name + "[hpTimes_join];"
            _fpgaKernelSetupCode += "for (int i = 0; i < hpTimes_join; i++) {"
            _fpgaKernelSetupCode += "    " + kernel_name + "[i] = krnlEngine(program_h, q_h, \"gqeJoin\");"
            _fpgaKernelSetupCode += "}"
            _fpgaKernelSetupCode += "for (int i = 0; i < hpTimes_join; i++) {"
            _fpgaKernelSetupCode += "    " + kernel_name + "[i].setup(" + join_left_table_name + "_partition_array[i], " + join_right_table_name + "_partition_array[i], " + _fpgaOutputTableName + "_partition_array" + "[i], " + nodeCfgCmd + ", buftmp_h);"
            _fpgaKernelSetupCode += "}"
          }
          else {
            kernel_name = "krnl_" + nodeOpName
            _fpgaKernelSetupCode += "krnlEngine " + kernel_name + ";"
            _fpgaKernelSetupCode += kernel_name + " = krnlEngine(program_h, q_h, \"gqeJoin\");"
            // Single child operation - use empty buffer as the right-hand table
            if (_children.length == 1) {
              var emptyBufferB_name = "tbl_" + nodeOpName + "_emptyBufferB"
              _fpgaOutputCode += "Table " + emptyBufferB_name + "(\"" + emptyBufferB_name + "\", 1, 8, \"\");"
              _fpgaOutputCode += emptyBufferB_name + ".allocateHost();"
              _fpgaOutputDevAllocateCode += emptyBufferB_name + ".allocateDevBuffer(context_h, 32);"
              _fpgaKernelSetupCode += kernel_name + ".setup(" + join_left_table_name + ", " + emptyBufferB_name + ", " + _fpgaOutputTableName + ", " + nodeCfgCmd + ", buftmp_h);"
            } else {
              _fpgaKernelSetupCode += kernel_name + ".setup(" + join_left_table_name + ", " + join_right_table_name + ", " + _fpgaOutputTableName + ", " + nodeCfgCmd + ", buftmp_h);"
            }
          }
          // TODO: Set up transfer engine
          _fpgaTransEngineName = "trans_" + nodeOpName
          _fpgaTransEngineSetupCode += "transEngine " + _fpgaTransEngineName + ";"
          _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".setq(q_h);"
          if (sf == 30) {
            if ((leftmost_operator != null && leftmost_operator._children.last._cpuORfpga == 0) ||
              (rightmost_operator != null && rightmost_operator._children.head._cpuORfpga == 0)) {
              _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".add(&(" + nodeCfgCmd_part + "));"
            }
          }
          _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".add(&(" + nodeCfgCmd + "));"
          for (ch <- _children) {
            if (ch._operation.isEmpty) {
              _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".add(&(" + ch._fpgaOutputTableName + "));"
            }
          }
          // TODO: Set up transfer engine output
          var fpgaTransEngineOutputName = "trans_" + nodeOpName + "_out"
          if ((parentNode == null) || ((parentNode != null) && ((parentNode.cpuORfpga == 1 && parentNode.fpgaOverlayType != this._fpgaOverlayType) || (parentNode.cpuORfpga == 0)))) {
            _fpgaTransEngineSetupCode += "transEngine " + fpgaTransEngineOutputName + ";"
            _fpgaTransEngineSetupCode += fpgaTransEngineOutputName + ".setq(q_h);"
          }
          _fpgaTransEngineSetupCode += "q_h.finish();"
          // TODO: Set up events
          var h2d_wr_name = "events_h2d_wr_" + nodeOpName
          var d2h_rd_name = "events_d2h_rd_" + nodeOpName
          var events_name = "events_" + nodeOpName
          _fpgaEventsH2DName = h2d_wr_name
          _fpgaEventsD2HName = d2h_rd_name
          _fpgaEventsName = events_name
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + h2d_wr_name + ";"
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + d2h_rd_name + ";"
          if (sf == 30) {
            _fpgaKernelEventsCode += "std::vector<cl::Event> " + events_name + "[2];"
          } else {
            _fpgaKernelEventsCode += "std::vector<cl::Event> " + events_name + ";"
          }
          _fpgaKernelEventsCode += h2d_wr_name + ".resize(1);"
          _fpgaKernelEventsCode += d2h_rd_name + ".resize(1);"
          if (sf == 30) {
            var num_gqe_part_events = 0
            if (leftmost_operator._children.last._cpuORfpga == 0 || leftmost_operator._children.last._nodeType == "SerializeFromObject") {
              num_gqe_part_events += 1
            }
            if (_children.length > 1 && (rightmost_operator._children.head._cpuORfpga == 0 || rightmost_operator._children.head._nodeType == "SerializeFromObject")) {
              num_gqe_part_events += 1
            }
            _fpgaKernelEventsCode += events_name + "[0].resize(" + num_gqe_part_events.toString + ");"
            _fpgaKernelEventsCode += events_name + "[1].resize(hpTimes_join);"
          } else {
            _fpgaKernelEventsCode += events_name + ".resize(1);"
          }
          var events_grp_name = "events_grp_" + nodeOpName
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + events_grp_name + ";"
          var prev_events_grp_name = "prev_events_grp_" + nodeOpName
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + prev_events_grp_name + ";"
          for (ch <- _children) {
            if (ch.operation.nonEmpty && ch._cpuORfpga == 1 && this._fpgaOverlayType == ch.fpgaOverlayType) {
              _fpgaKernelRunCode += prev_events_grp_name + ".push_back(" + ch.fpgaEventsH2DName + "[0]);"
            }
          }
          // TODO: Run Xilinx L2 module kernels and link events
          for (ch <- _children) {
            if ((ch.cpuORfpga == 0) || ((ch.cpuORfpga == 1 && this._fpgaOverlayType != ch.fpgaOverlayType))) {
              if (ch._operation.nonEmpty) {
                _fpgaKernelRunCode += _fpgaTransEngineName + ".add(&(" + ch._fpgaOutputTableName + "));"
              }
            }
          }
          _fpgaKernelRunCode += _fpgaTransEngineName + ".host2dev(0, &(" + prev_events_grp_name + "), &(" + _fpgaEventsH2DName + "[0]));"
          _fpgaKernelRunCode += events_grp_name + ".push_back(" + _fpgaEventsH2DName + "[0]);"
          for (ch <- _children) {
            if (ch.operation.nonEmpty && ch._cpuORfpga == 1 && this._fpgaOverlayType == ch.fpgaOverlayType) {
              if (sf == 30) {
                _fpgaKernelRunCode += events_grp_name + ".insert(std::end(" + events_grp_name + "), std::begin(" + ch.fpgaEventsName + "[0]), std::end(" + ch.fpgaEventsName + "[0]));"
                _fpgaKernelRunCode += events_grp_name + ".insert(std::end(" + events_grp_name + "), std::begin(" + ch.fpgaEventsName + "[1]), std::end(" + ch.fpgaEventsName + "[1]));"
              } else {
                _fpgaKernelRunCode += events_grp_name + ".push_back(" + ch.fpgaEventsName + "[0]);"
              }
            }
          }
          if (sf == 30) {
            var num_gqe_part_events = 0
            if (joinTblOrderSwapped == false) {
              if (leftmost_operator._children.head._cpuORfpga == 0 || leftmost_operator._children.head._nodeType == "SerializeFromObject") {
                _fpgaKernelRunCode += kernel_name_left + ".run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[0][" + num_gqe_part_events.toString + "]));"
                num_gqe_part_events += 1
              }
              if (_children.length > 1 && (rightmost_operator._children.last._cpuORfpga == 0 || rightmost_operator._children.last._nodeType == "SerializeFromObject")) {
                _fpgaKernelRunCode += kernel_name_right + ".run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[0][" + num_gqe_part_events.toString + "]));"
              }
            } else {
              if (leftmost_operator._children.last._cpuORfpga == 0 || leftmost_operator._children.last._nodeType == "SerializeFromObject") {
                _fpgaKernelRunCode += kernel_name_left + ".run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[0][" + num_gqe_part_events.toString + "]));"
                num_gqe_part_events += 1
              }
              if (_children.length > 1 && (rightmost_operator._children.head._cpuORfpga == 0 || rightmost_operator._children.head._nodeType == "SerializeFromObject")) {
                _fpgaKernelRunCode += kernel_name_right + ".run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[0][" + num_gqe_part_events.toString + "]));"
              }
            }
            _fpgaKernelRunCode += "for (int i(0); i < hpTimes_join; ++i) {"
            if (num_gqe_part_events == 0) {
              _fpgaKernelRunCode += "    " + kernel_name + "[i].run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[1][i]));"
            } else {
              _fpgaKernelRunCode += "    " + kernel_name + "[i].run(0, &(" + _fpgaEventsName + "[0]), &(" + _fpgaEventsName + "[1][i]));"
            }
            _fpgaKernelRunCode += "}"
          } else {
            _fpgaKernelRunCode += kernel_name + ".run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[0]));"
          }

          if (parentNode == null) {
            _fpgaKernelRunCode += ""
            if (sf == 30) {
              _fpgaKernelRunCode += "for (int i(0); i < hpTimes_join; ++i) {"
              _fpgaKernelRunCode += "    " + fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName + "_partition_array" + "[i]));"
              _fpgaKernelRunCode += "}"
              _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "[1]), &(" + _fpgaEventsD2HName + "[0]));"
            } else {
              _fpgaKernelRunCode += fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName + "));"
              _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "), &(" + _fpgaEventsD2HName + "[0]));"
            }
            _fpgaKernelRunCode += "q_h.flush();"
            _fpgaKernelRunCode += "q_h.finish();"
          }
          if (parentNode != null) {
            // two consecutive overlay calls are for different overlay types - TODO: add support for when two overlay call types are the same but on two different FPGA devices
            // special case: outer join
            if (this._nodeType == "JOIN_LEFTANTI" && this._children(0)._nodeType == "JOIN_INNER" && this._joining_expression == this._children(0)._joining_expression) {
              _fpgaKernelRunCode += ""
              if (sf == 30) {
                _fpgaKernelRunCode += "for (int i(0); i < hpTimes_join; ++i) {"
                _fpgaKernelRunCode += "    " + fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName + "_partition_array" + "[i]));"
                _fpgaKernelRunCode += "    " + fpgaTransEngineOutputName + ".add(&(" + this._children(0)._fpgaOutputTableName + "_partition_array" + "[i]));"
                _fpgaKernelRunCode += "}"
                _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "[1]), &(" + _fpgaEventsD2HName + "[0]));"
              } else {
                _fpgaKernelRunCode += fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName + "));"
                _fpgaKernelRunCode += fpgaTransEngineOutputName + ".add(&(" + this._children(0)._fpgaOutputTableName + "));"
                _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "), &(" + _fpgaEventsD2HName + "[0]));"
              }
              _fpgaKernelRunCode += "q_h.flush();"
              _fpgaKernelRunCode += "q_h.finish();"

              // combine inner and anti join tables results as the outer join table results
              var tmp_join_col_concatenate = "SW_" + nodeOpName + "_concatenate("
              tmp_join_col_concatenate += _fpgaOutputTableName + ", " + this._children(0)._fpgaOutputTableName + ");"
              _fpgaKernelRunCode += tmp_join_col_concatenate

              var sw_join_concatenate_func_cal = "void " + "SW_" + nodeOpName + "_concatenate("
              sw_join_concatenate_func_cal += "Table &" + _fpgaOutputTableName + ", "
              sw_join_concatenate_func_cal += "Table &" + this._children(0)._fpgaOutputTableName
              sw_join_concatenate_func_cal += ") {"
              joinConcatenateFuncCode += sw_join_concatenate_func_cal
              joinConcatenateFuncCode += "    int start_idx = " + _fpgaOutputTableName + ".getNumRow();"
              joinConcatenateFuncCode += "    int nrow = " + this._children(0)._fpgaOutputTableName + ".getNumRow();"
              joinConcatenateFuncCode += "    int i = 0;"
              joinConcatenateFuncCode += "    for (int r(start_idx); r<start_idx+nrow; ++r) {"
              var col_idx = 0
              for (o_col <- this._outputCols) {
                var output_col_type = getColumnType(o_col, dfmap)
                if (output_col_type == "IntegerType") {
                  joinConcatenateFuncCode += "        int32_t " + stripColumnName(o_col) + " = " + this._children(0)._fpgaOutputTableName + ".getInt32(i, " + col_idx.toString + ");"
                  joinConcatenateFuncCode += "        " + _fpgaOutputTableName + ".setInt32(r, " + col_idx.toString + ", " + stripColumnName(o_col) + ");"
                }
                else {
                  joinConcatenateFuncCode += "        Error: unsupported data type - revisit cpu/fpga determination logic"
                }
                col_idx += 1
              }
              joinConcatenateFuncCode += "        i++;"
              joinConcatenateFuncCode += "    }"
              joinConcatenateFuncCode += "    " + _fpgaOutputTableName + ".setNumRow(start_idx + nrow);"
              joinConcatenateFuncCode += "    std::cout << \"" + _fpgaOutputTableName + " #Row: \" << " + _fpgaOutputTableName + ".getNumRow() << std::endl;"
              joinConcatenateFuncCode += "}"

              for (line <- joinConcatenateFuncCode) {
                _fpgaSWFuncCode += line
              }

              // transfer table data to another table to be used in a different 'context'
              if (parentNode.fpgaOverlayType != this._fpgaOverlayType) {
                _fpgaKernelRunCode += _fpgaOutputTableName + ".copyTableData(&(" + _fpgaOutputTableName.stripSuffix("_preprocess") + "));"
                _fpgaOutputTableName = _fpgaOutputTableName.stripSuffix("_preprocess")
              }
            }
            else if ((parentNode.cpuORfpga == 1 && parentNode.fpgaOverlayType != this._fpgaOverlayType) || (parentNode.cpuORfpga == 0)) {
              _fpgaKernelRunCode += ""
              if (sf == 30) {
                _fpgaKernelRunCode += "for (int i(0); i < hpTimes_join; ++i) {"
                _fpgaKernelRunCode += "    " + fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName + "_partition_array" + "[i]));"
                _fpgaKernelRunCode += "}"
                _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "[1]), &(" + _fpgaEventsD2HName + "[0]));"
              } else {
                _fpgaKernelRunCode += fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName + "));"
                _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "), &(" + _fpgaEventsD2HName + "[0]));"
              }
              _fpgaKernelRunCode += "q_h.flush();"
              _fpgaKernelRunCode += "q_h.finish();"

              if (parentNode._fpgaOverlayType != this._fpgaOverlayType && parentNode.cpuORfpga != 0) {
                // transfer table data to another table to be used in a different 'context'
                _fpgaKernelRunCode += "WaitForEvents(" + _fpgaEventsD2HName + ");"
                _fpgaKernelRunCode += _fpgaOutputTableName + ".copyTableData(&(" + _fpgaOutputTableName.stripSuffix("_preprocess") + "));"
                _fpgaOutputTableName = _fpgaOutputTableName.stripSuffix("_preprocess")
              }
            }
          }
        }
        else if (_fpgaOverlayType == 1) { // gqe_join-0, gqe_aggr-1, gqe_part-2
          // tag:overlay1
          var nodeCfgCmd = "cfg_" + nodeOpName + "_cmds"
          var nodeGetCfgFuncName = "get_cfg_dat_" + nodeOpName + "_gqe_aggr"
          _fpgaConfigCode += "AggrCfgCmd " + nodeCfgCmd + ";"
          _fpgaConfigCode += nodeCfgCmd + ".allocateHost();"
          _fpgaConfigCode += nodeGetCfgFuncName + "(" + nodeCfgCmd + ".cmd);"
          _fpgaConfigCode += nodeCfgCmd + ".allocateDevBuffer(context_a, 32);"

          var nodeCfgCmd_out = "cfg_" + nodeOpName + "_cmds_out"
          _fpgaConfigCode += "AggrCfgCmd " + nodeCfgCmd_out + ";"
          _fpgaConfigCode += nodeCfgCmd_out + ".allocateHost();"
          _fpgaConfigCode += nodeCfgCmd_out + ".allocateDevBuffer(context_a, 33);"

          // Generate the function that generates the Xilinx L2 module configurations
          var aggrConsolidateFuncCode = new ListBuffer[String]()
          var cfgFuncCode = new ListBuffer[String]()
          cfgFuncCode += "void " + nodeGetCfgFuncName + "(ap_uint<32>* buf) {"
          var cfgFuncCode_gqePart = new ListBuffer[String]()
          if (sf == 30) {
            cfgFuncCode_gqePart += "void " + nodeGetCfgFuncName + "_part" + "(ap_uint<512>* hbuf) {"
          }
          var innerMostOperator = getInnerMostBindedOperator(this)
          var input_table_col = innerMostOperator._children.head.outputCols
          var input_table_name = innerMostOperator._children.head._fpgaOutputTableName
          var nodeCfgCmd_part = "cfg_" + nodeOpName + "_cmds_part"
          if (sf == 30) {
            if (innerMostOperator._children.head._cpuORfpga == 0 || innerMostOperator._children.head._nodeType == "SerializeFromObject"){
              _fpgaConfigCode += "cfgCmd " + nodeCfgCmd_part + ";"
              _fpgaConfigCode += nodeCfgCmd_part + ".allocateHost();"
              _fpgaConfigCode += nodeGetCfgFuncName + "_part" + " (" + nodeCfgCmd_part + ".cmd);"
              _fpgaConfigCode += nodeCfgCmd_part + ".allocateDevBuffer(context_a, 32);"
            }
          }
          // ----------------------------------- Debug info -----------------------------------
          cfgFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
          cfgFuncCode += "    // Supported operation: " + _nodeType
          cfgFuncCode += "    // Operation: " + _operation
          for (binded_overlay <- _bindedOverlayInstances) {
            cfgFuncCode += "        // Binded Operation: " + binded_overlay._nodeType + " -> operations: " + binded_overlay._operation
          }
          for (binded_overlay <- _bindedOverlayInstances_left) {
            cfgFuncCode += "        // Binded Operation: " + binded_overlay._nodeType + " -> operations: " + binded_overlay._operation
          }
          for (binded_overlay <- _bindedOverlayInstances_right) {
            cfgFuncCode += "        // Binded Operation: " + binded_overlay._nodeType + " -> operations: " + binded_overlay._operation
          }
          cfgFuncCode += "    // Input Table: " + input_table_col
          cfgFuncCode += "    // Output Table: " + _outputCols
          cfgFuncCode += "    // Node Depth: " + _treeDepth

          cfgFuncCode += "    ap_uint<32>* config = buf;"
          cfgFuncCode += "    memset(config, 0, sizeof(ap_uint<32>) * 83);"
          if (sf == 30) {
            cfgFuncCode_gqePart += "    ap_uint<512>* b = hbuf;"
            cfgFuncCode_gqePart += "    memset(b, 0, sizeof(ap_uint<512>) * 9);"
            cfgFuncCode_gqePart += "    ap_uint<512> t = 0;"
            cfgFuncCode_gqePart += ""
          }

          // A record to track how inputs cols navigates through the overlay
          var col_idx_dict_prev = collection.mutable.Map[String, Int]()
          var idx_col_dict_prev = collection.mutable.Map[Int, String]()
          var tmp_idx = 0
          for (i_col <- input_table_col) {
            col_idx_dict_prev += (i_col -> tmp_idx)
            idx_col_dict_prev += (tmp_idx -> i_col)
            tmp_idx += 1
          }

          //eval0 - Alec-added
          cfgFuncCode += ""
          cfgFuncCode += "    // eval0"
          var eval0_func = "NOP"
          var eval0_func_call = "// eval0: NOP"
          var groupby_operator = getGroupByAggregateOperator(this)
          var col_idx_dict_next = col_idx_dict_prev.clone()
          var idx_col_dict_next = idx_col_dict_prev.clone()
          if (groupby_operator != null && groupby_operator._aggregate_expression.nonEmpty) {
            // get first aggr expr (w/ evaluation)
            var aggr_expr = groupby_operator._aggregate_expression(0)
            var first_expr = true
            var eval_found = false
            for (eval_expr <- groupby_operator._aggregate_expression) {
              if (!isPureAggrOperation_sub(eval_expr) && first_expr && !eval_found) {
                aggr_expr = eval_expr
                first_expr = false
                eval_found = true
              }
            }
            if (eval_found) {
              // move all referenced cols in aggr expr to the front
              var start_idx = 0
              for (ref_col <- aggr_expr.references) {
                // place reference col and idx to 'start_idx'
                var prev_idx = col_idx_dict_next(ref_col.toString)
                col_idx_dict_next(ref_col.toString) = start_idx
                var prev_col = idx_col_dict_next(start_idx)
                idx_col_dict_next(start_idx) = ref_col.toString
                // place the previous col and idx at new location
                col_idx_dict_next(prev_col) = prev_idx
                idx_col_dict_next(prev_idx) = prev_col
                // increment on the next front idx
                start_idx += 1
              }
              // eval dynamicALUCompiler function call
              var tmp_aggr_expr = new ListBuffer[Expression]()
              tmp_aggr_expr += aggr_expr
              if (isPureAggrOperation(tmp_aggr_expr)) {
                eval0_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                eval0_func_call = "// eval0: NOP"
              }
              else {
                eval0_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                var aggr_const_i = new Array[Int](4) // default set as zero
                var strm_order_i = 1
                var (aggr_str, aggr_const, strm_order) = getAggregateExpression_ALUOPCompiler_cmd(aggr_expr, aggr_const_i, strm_order_i)
                aggr_str = aggr_str.stripPrefix("(")
                aggr_str = aggr_str.stripSuffix(")")
                var ALUOPCompiler = "xf::database::dynamicALUOPCompiler<uint32_t, uint32_t, uint32_t, uint32_t>(\""
                ALUOPCompiler += aggr_str + "\", "
                ALUOPCompiler += aggr_const(0) + ", " + aggr_const(1) + ", " + aggr_const(2) + ", " + aggr_const(3) + ", "
                ALUOPCompiler += "op_eval_0);"
                eval0_func_call = ALUOPCompiler

                // add aliase col to col_idx and idx_col table
                var aliase_col = aggr_expr.toString.split(" AS ").last
                col_idx_dict_next += (aliase_col -> 8)
                idx_col_dict_next += (8 -> aliase_col)
              }
            }
            else {
              cfgFuncCode += "    // NO evaluation 0 in aggregation expression - eval0"
            }
          }
          else {
            cfgFuncCode += "    // NO aggregation operation - eval0"
          }

          cfgFuncCode += "    ap_uint<32> t;"
          var i_tbl_col_list = "signed char id[] = {"
          // input col assignment
          for (col_idx <- 0 to 8-1) { //max 8 cols for input table
            if (col_idx < idx_col_dict_prev.size) { // input cols
              var col_name = idx_col_dict_next(col_idx)
              var prev_col_idx = col_idx_dict_prev(col_name)
              i_tbl_col_list += prev_col_idx.toString + ","
            }
            else {
              i_tbl_col_list += "-1,"
            }
          }
          i_tbl_col_list = i_tbl_col_list.stripSuffix(",")
          i_tbl_col_list += "};"
          cfgFuncCode += "    " + i_tbl_col_list
          cfgFuncCode += "    for (int c = 0; c < 4; ++c) {"
          cfgFuncCode += "        t.range(8 * c + 7, 8 * c) = id[c];"
          cfgFuncCode += "    }"
          cfgFuncCode += "    config[0] = t;"
          cfgFuncCode += "    for (int c = 0; c < 4; ++c) {"
          cfgFuncCode += "        t.range(8 * c + 7, 8 * c) = id[c + 4];"
          cfgFuncCode += "    }"
          cfgFuncCode += "    config[1] = t;"
          cfgFuncCode += ""
          cfgFuncCode += "    ap_uint<289> op_eval_0 = 0;" + " // " + eval0_func
          cfgFuncCode += "    " + eval0_func_call
          cfgFuncCode += "    for (int i = 0; i < 9; i++) {"
          cfgFuncCode += "        config[i + 2] = op_eval_0(32 * (i + 1) - 1, 32 * i);"
          cfgFuncCode += "    }"
          cfgFuncCode += "    config[11] = op_eval_0[288];"
          // update prev with next
          col_idx_dict_prev = col_idx_dict_next.clone()
          idx_col_dict_prev = idx_col_dict_next.clone()

          //eval1 - Alec-added
          cfgFuncCode += ""
          cfgFuncCode += "    // eval0 -> eval1"
          var eval1_func = "NOP"
          var eval1_func_call = "// eval1: NOP"
          if (groupby_operator != null && groupby_operator._aggregate_expression.nonEmpty) {
            // get second aggr expr (w/ evaluation)
            var aggr_expr = groupby_operator._aggregate_expression(0)
            var first_expr = true
            var second_expr = true
            var eval_found = false
            for (eval_expr <- groupby_operator._aggregate_expression) {
              if (!isPureAggrOperation_sub(eval_expr)) {
                if (first_expr) {
                  first_expr = false
                }
                else {
                  if (second_expr && !eval_found) {
                    aggr_expr = eval_expr
                    eval_found = true
                  }
                }
              }
              else { //pure aggregation cols e.g., sum(col_name)
                var aliase_col = eval_expr.toString.split(" AS ").last
                var ref_col_idx = -1
                if (eval_expr.references.nonEmpty) {
                  var ref_col = eval_expr.references.head.toString
                  ref_col_idx = col_idx_dict_next(ref_col)
                }
                col_idx_dict_next += (aliase_col -> ref_col_idx)
              }
            }
            if (eval_found) {
              //move all referenced cols in aggr expr to the front
              var start_idx = 0
              for (ref_col <- aggr_expr.references) {
                // place reference col and idx to 'start_idx'
                var prev_idx = col_idx_dict_next(ref_col.toString)
                col_idx_dict_next(ref_col.toString) = start_idx
                var prev_col = idx_col_dict_next(start_idx)
                idx_col_dict_next(start_idx) = ref_col.toString
                // place the previous col and idx at new location
                col_idx_dict_next(prev_col) = prev_idx
                idx_col_dict_next(prev_idx) = prev_col
                // increment on the next front idx
                start_idx += 1
              }

              // move eval0 result from 8th col to append after the normal cols
              var eval0_result_col_name = idx_col_dict_next(8)
              col_idx_dict_next(eval0_result_col_name) = idx_col_dict_next.size - 1
              idx_col_dict_next.remove(8)
              idx_col_dict_next += (idx_col_dict_next.size -> eval0_result_col_name)

              // eval dynamicALUCompiler function call
              var tmp_aggr_expr = new ListBuffer[Expression]()
              tmp_aggr_expr += aggr_expr
              if (isPureAggrOperation(tmp_aggr_expr)) {
                eval1_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                eval1_func_call = ""
              }
              else {
                eval1_func = getAggregateExpression_ALUOPCompiler(aggr_expr)
                var aggr_const_i = new Array[Int](4) // default set as zero
                var strm_order_i = 1
                var (aggr_str, aggr_const, strm_order) = getAggregateExpression_ALUOPCompiler_cmd(aggr_expr, aggr_const_i, strm_order_i)
                aggr_str = aggr_str.stripPrefix("(")
                aggr_str = aggr_str.stripSuffix(")")
                var ALUOPCompiler = "xf::database::dynamicALUOPCompiler<uint32_t, uint32_t, uint32_t, uint32_t>(\""
                ALUOPCompiler += aggr_str + "\", "
                ALUOPCompiler += aggr_const(0) + ", " + aggr_const(1) + ", " + aggr_const(2) + ", " + aggr_const(3) + ", "
                ALUOPCompiler += "op_eval_1);"
                eval1_func_call = ALUOPCompiler

                // add aliase col to col_idx and idx_col table
                var aliase_col = aggr_expr.toString.split(" AS ").last
                col_idx_dict_next += (aliase_col -> 8)
                idx_col_dict_next += (8 -> aliase_col)
              }
            }
            else {
              cfgFuncCode += "    // NO evaluation 1 in aggregation expression - eval1"
              // if eval0 != null, move eval0 result from 8th col to append after the normal cols
              if (idx_col_dict_next.contains(8)) {
                var eval0_result_col_name = idx_col_dict_next(8)
                col_idx_dict_next(eval0_result_col_name) = idx_col_dict_next.size-1
                idx_col_dict_next.remove(8)
                idx_col_dict_next += (idx_col_dict_next.size -> eval0_result_col_name)
              }
            }
          }
          else {
            cfgFuncCode += "    // NO aggregation operation - eval1"
            // if eval0 != null, move eval0 result from 8th col to append after the normal cols
            if (idx_col_dict_next.contains(8)) {
              var eval0_result_col_name = idx_col_dict_next(8)
              col_idx_dict_next(eval0_result_col_name) = idx_col_dict_next.size-1
              idx_col_dict_next.remove(8)
              idx_col_dict_next += (idx_col_dict_next.size -> eval0_result_col_name)
            }
          }

          // input col assignment
          cfgFuncCode += "    ap_int<64> shuffle1_cfg;"
          for (ss1 <- 0 to 8 - 1) {
            if (ss1 < col_idx_dict_prev.size) {
              if (idx_col_dict_prev.contains(ss1)) { // normal cols
                var col_name = idx_col_dict_next(ss1)
                var prev_col_idx = col_idx_dict_prev(col_name)
                cfgFuncCode += "    shuffle1_cfg(" + ((ss1 + 1) * 8 - 1).toString + ", " + (ss1 * 8).toString + ") = " + prev_col_idx.toString + ";" + " // " + col_name
              } else { //eval col - always at idx 8
                cfgFuncCode += "    shuffle1_cfg(" + ((ss1 + 1) * 8 - 1).toString + ", " + (ss1 * 8).toString + ") = " + "8" + ";" + " // " + idx_col_dict_prev(8)
              }
            } else {
              cfgFuncCode += "    shuffle1_cfg(" + ((ss1 + 1) * 8 - 1).toString + ", " + (ss1 * 8).toString + ") = -1;"
            }
          }
          cfgFuncCode += "    config[67] = shuffle1_cfg(31, 0);"
          cfgFuncCode += "    config[68] = shuffle1_cfg(63, 32);"
          cfgFuncCode += ""
          cfgFuncCode += "    ap_uint<289> op_eval_1 = 0;" + " // " + eval1_func
          cfgFuncCode += "    " + eval1_func_call
          cfgFuncCode += "    for (int i = 0; i < 9; i++) {"
          cfgFuncCode += "        config[i + 12] = op_eval_1(32 * (i + 1) - 1, 32 * i);"
          cfgFuncCode += "    }"
          cfgFuncCode += "    config[21] = op_eval_1[288];"
          // update prev with next
          col_idx_dict_prev = col_idx_dict_next.clone()
          idx_col_dict_prev = idx_col_dict_next.clone()

          //filter - Alec-added
          cfgFuncCode += ""
          cfgFuncCode += "    // eval1 -> filter"
          var filter_operator = getFilterOperator(this)
          var filter_config_func_call = ""
          if (filter_operator == null || filter_operator._filtering_expression == null) {
            filter_config_func_call = "gen_pass_fcfg"
          }
          else {
            // TODO: re-shuffle input col to be situated in the first 4 col
            var tmp_idx = 0
            for (ref_col <- filter_operator._filtering_expression.references) {
              var prev_ref_idx = col_idx_dict_next(ref_col.toString)
              var tmp_col = idx_col_dict_next(tmp_idx)
              col_idx_dict_next(tmp_col) = prev_ref_idx
              idx_col_dict_next(prev_ref_idx) = ref_col.toString
              col_idx_dict_next(ref_col.toString) = tmp_idx
              idx_col_dict_next(tmp_idx) = ref_col.toString
              tmp_idx += 1
            }
            var filterCfgFuncName = "gen_fcfg_" + _fpgaNodeName
            filter_config_func_call = filterCfgFuncName
            var filterCfgFuncCode = getFilterConfigFuncCode(filterCfgFuncName, filter_operator, col_idx_dict_next)
            for (line <- filterCfgFuncCode) {
              _fpgaConfigFuncCode += line
            }
          }
          // input col assignment
          cfgFuncCode += "    ap_int<64> shuffle2_cfg;"
          for (ss2 <- 0 to 8 - 1) {
            if (ss2 < col_idx_dict_prev.size) {
              if (idx_col_dict_prev.contains(ss2)) { // normal cols
                var col_name = idx_col_dict_next(ss2)
                var prev_col_idx = col_idx_dict_prev(col_name)
                cfgFuncCode += "    shuffle2_cfg(" + ((ss2 + 1) * 8 - 1).toString + ", " + (ss2 * 8).toString + ") = " + prev_col_idx.toString + ";" + " // " + col_name
              }
              else if (idx_col_dict_prev.contains(8)) { //eval col - always at idx 8
                cfgFuncCode += "    shuffle2_cfg(" + ((ss2 + 1) * 8 - 1).toString + ", " + (ss2 * 8).toString + ") = " + "8" + ";" + " // " + idx_col_dict_prev(8)
              }
              else {
                cfgFuncCode += "    shuffle2_cfg(" + ((ss2 + 1) * 8 - 1).toString + ", " + (ss2 * 8).toString + ") = -1;"
              }
            } else {
              cfgFuncCode += "    shuffle2_cfg(" + ((ss2 + 1) * 8 - 1).toString + ", " + (ss2 * 8).toString + ") = -1;"
            }
          }
          cfgFuncCode += "    config[69] = shuffle2_cfg(31, 0);"
          cfgFuncCode += "    config[70] = shuffle2_cfg(63, 32);"
          cfgFuncCode += ""
          cfgFuncCode += "    uint32_t fcfg[45];"
          cfgFuncCode += "    " + filter_config_func_call + "(fcfg);"
          cfgFuncCode += "    memcpy(&config[22], fcfg, sizeof(uint32_t) * 45);"
          // update prev with next
          col_idx_dict_prev = col_idx_dict_next.clone()
          idx_col_dict_prev = idx_col_dict_next.clone()

          //groupBy - Alec-added
          cfgFuncCode += ""
          cfgFuncCode += "    // filter -> groupBy"
          // input key col assignment
          if (sf == 30) {
            //gqePart configuration bits
            var filter_a_config_call = "gen_pass_fcfg";
            cfgFuncCode_gqePart += "    // input table a"
            var tbl_A_col_list = "signed char id_a[] = {"
            var col_name = ""
            var col_idx = -1
            var col_counter = 0
            for (key_col <- groupby_operator._groupBy_operation) { // key cols
              col_idx = col_idx_dict_prev(key_col)
              tbl_A_col_list += col_idx.toString + ","
              col_counter += 1
            }
            for (o_col <- groupby_operator._outputCols) {
              if (!groupby_operator._groupBy_operation.contains(o_col)) { // pld cols
                col_idx = col_idx_dict_prev(o_col)
                tbl_A_col_list += col_idx.toString + ","
                col_counter += 1
              }
            }
            while(col_counter < 8) {
              tbl_A_col_list += "-1,"
              col_counter += 1
            }
            tbl_A_col_list = tbl_A_col_list.stripSuffix(",")
            tbl_A_col_list += "};"
            cfgFuncCode_gqePart += "    " + tbl_A_col_list
            cfgFuncCode_gqePart += "    for (int c = 0; c < 8; ++c) {"
            cfgFuncCode_gqePart += "        t.range(56 + 8 * c + 7, 56 + 8 * c) = id_a[c];"
            cfgFuncCode_gqePart += "    }"
            cfgFuncCode_gqePart += "    // filter tbl_a config"
            cfgFuncCode_gqePart += "    uint32_t cfga[45];"
            cfgFuncCode_gqePart += "    " + filter_a_config_call + "(cfga);"
            cfgFuncCode_gqePart += "    memcpy(&b[3], cfga, sizeof(uint32_t) * 45);"
            var filter_b_config_call = "gen_pass_fcfg";
            cfgFuncCode_gqePart += ""
            cfgFuncCode_gqePart += "    // input table b"
            var tbl_B_col_list = "signed char id_b[] = {"
            for (b <- 0 to 8 - 1) {
              tbl_B_col_list += "-1,"
            }
            tbl_B_col_list = tbl_B_col_list.stripSuffix(",")
            tbl_B_col_list += "};"
            cfgFuncCode_gqePart += "    " + tbl_B_col_list
            cfgFuncCode_gqePart += "    for (int c = 0; c < 8; ++c) {"
            cfgFuncCode_gqePart += "        t.range(120 + 8 * c + 7, 120 + 8 * c) = id_b[c];"
            cfgFuncCode_gqePart += "    }"
            cfgFuncCode_gqePart += "    // filter tbl_b config"
            cfgFuncCode_gqePart += "    uint32_t cfgb[45];"
            cfgFuncCode_gqePart += "    " + filter_b_config_call + "(cfgb);"
            cfgFuncCode_gqePart += "    memcpy(&b[6], cfgb, sizeof(uint32_t) * 45);"
            cfgFuncCode_gqePart += ""
            cfgFuncCode_gqePart += "    // join config"
            cfgFuncCode_gqePart += "    t.set_bit(0, 1);    // join"
            var dual_key_join_on = 0
            if (groupby_operator._groupBy_operation.length == 2) {
              dual_key_join_on = 1
            } else {
              dual_key_join_on = 0
            }
            cfgFuncCode_gqePart += "    t.set_bit(2, " + dual_key_join_on + ");    // dual-key"
            cfgFuncCode_gqePart += "    t.range(5, 3) = 0;  // hash join flag = 0 for normal, 1 for semi, 2 for anti"
            cfgFuncCode_gqePart += ""
            cfgFuncCode_gqePart += "    b[0] = t;"
            cfgFuncCode_gqePart += "}"
            //gqeAggr configuration bits
            cfgFuncCode += "    ap_int<64> shuffle3_cfg;"
            for (ss3 <- 0 to 8 - 1) {
              if (ss3 < groupby_operator._groupBy_operation.length) {
                var col_name = groupby_operator._groupBy_operation(ss3)
                var col_idx = col_idx_dict_prev(col_name)
                cfgFuncCode += "    shuffle3_cfg(" + ((ss3 + 1) * 8 - 1).toString + ", " + (ss3 * 8).toString + ") = " + col_idx.toString + ";" + " // " + col_name
              } else {
                cfgFuncCode += "    shuffle3_cfg(" + ((ss3 + 1) * 8 - 1).toString + ", " + (ss3 * 8).toString + ") = -1;"
              }
            }
            cfgFuncCode += "    config[71] = shuffle3_cfg(31, 0);"
            cfgFuncCode += "    config[72] = shuffle3_cfg(63, 32);"
            cfgFuncCode += ""
            // input pld col assignment
            cfgFuncCode += "    ap_int<64> shuffle4_cfg;"
            var tmp_col_idx = 0
            for (o_col <- groupby_operator._outputCols) {
              if (!groupby_operator._groupBy_operation.contains(o_col)) { // pld cols
                var prev_col_idx = col_idx_dict_prev(o_col)
                cfgFuncCode += "    shuffle4_cfg(" + ((tmp_col_idx + 1) * 8 - 1).toString + ", " + (tmp_col_idx * 8).toString + ") = " + prev_col_idx.toString + ";" + " // " + o_col
                tmp_col_idx += 1
              }
            }
            for (ss4 <- tmp_col_idx to 8 - 1) { // unused cols
              cfgFuncCode += "    shuffle4_cfg(" + ((ss4 + 1) * 8 - 1).toString + ", " + (ss4 * 8).toString + ") = -1;"
            }
            cfgFuncCode += "    config[73] = shuffle4_cfg(31, 0);"
            cfgFuncCode += "    config[74] = shuffle4_cfg(63, 32);"
            cfgFuncCode += ""

          } else {
            cfgFuncCode += "    ap_int<64> shuffle3_cfg;"
            for (ss3 <- 0 to 8 - 1) {
              if (ss3 < groupby_operator._groupBy_operation.length) {
                var col_name = groupby_operator._groupBy_operation(ss3)
                var col_idx = col_idx_dict_prev(col_name)
                cfgFuncCode += "    shuffle3_cfg(" + ((ss3 + 1) * 8 - 1).toString + ", " + (ss3 * 8).toString + ") = " + col_idx.toString + ";" + " // " + col_name
              } else {
                cfgFuncCode += "    shuffle3_cfg(" + ((ss3 + 1) * 8 - 1).toString + ", " + (ss3 * 8).toString + ") = -1;"
              }
            }
            cfgFuncCode += "    config[71] = shuffle3_cfg(31, 0);"
            cfgFuncCode += "    config[72] = shuffle3_cfg(63, 32);"
            cfgFuncCode += ""
            // input pld col assignment
            cfgFuncCode += "    ap_int<64> shuffle4_cfg;"
            var tmp_col_idx = 0
            for (o_col <- groupby_operator._outputCols) {
              if (!groupby_operator._groupBy_operation.contains(o_col)) { // pld cols
                var prev_col_idx = col_idx_dict_prev(o_col)
                cfgFuncCode += "    shuffle4_cfg(" + ((tmp_col_idx + 1) * 8 - 1).toString + ", " + (tmp_col_idx * 8).toString + ") = " + prev_col_idx.toString + ";" + " // " + o_col
                tmp_col_idx += 1
              }
            }
            for (ss4 <- tmp_col_idx to 8 - 1) { // unused cols
              cfgFuncCode += "    shuffle4_cfg(" + ((ss4 + 1) * 8 - 1).toString + ", " + (ss4 * 8).toString + ") = -1;"
            }
            cfgFuncCode += "    config[73] = shuffle4_cfg(31, 0);"
            cfgFuncCode += "    config[74] = shuffle4_cfg(63, 32);"
            cfgFuncCode += ""
          }
          // aggr_op assignment on pld cols
          cfgFuncCode += "    ap_uint<4> aggr_op[8] = {0, 0, 0, 0, 0, 0, 0, 0};"
          for (aggr_idx <- 0 to 8 - 1) {
            if (aggr_idx < groupby_operator._aggregate_expression.length) {
              var tmp_aggr_expr = groupby_operator._aggregate_expression(aggr_idx)
              if (isPureEvalOperation_sub(tmp_aggr_expr)) { // no aggrgation: TODO: is this possible?
                cfgFuncCode += "    aggr_op["+ aggr_idx.toString +"] = xf::database::enums::AOP_SUM;"
              } else {
                // grab aggr_op from _aggregate_expression[aggr_idx]
                var aggr_op_enum = getAggregateOpEnum(tmp_aggr_expr)
                cfgFuncCode += "    aggr_op["+ aggr_idx.toString +"] = " + aggr_op_enum + ";"
              }
            }
            else {
              cfgFuncCode += "    aggr_op["+ aggr_idx.toString +"] = xf::database::enums::AOP_SUM;"
            }
          }
          cfgFuncCode += "    config[75] = (aggr_op[7], aggr_op[6], aggr_op[5], aggr_op[4], aggr_op[3], aggr_op[2], aggr_op[1], aggr_op[0]);"
          cfgFuncCode += ""
          // # of col types
          var num_keys = groupby_operator._groupBy_operation.length
          var num_plds = groupby_operator._aggregate_operation.length
          cfgFuncCode += "    config[76] = " + (num_keys).toString + "; //# key col"
          cfgFuncCode += "    config[77] = " + (num_plds).toString + "; //# pld col"
          cfgFuncCode += "    config[78] = 0; //# aggr num"
          cfgFuncCode += ""
          // column merge selection
          cfgFuncCode += "    ap_uint<8> merge[5];"
          // merge selection
          var key_col_selection = "0b"
          var pld_col_selection = "0b"
          for (merg_col <- 0 to 8-1) {
            if (merg_col < num_keys) {
              key_col_selection += "1"
            } else {
              key_col_selection += "0"
            }
            var pld_idx = 8-merg_col
            var pld_aggr_enum = "xf::database::enums::AOP_SUM"
            if (pld_idx <= num_plds) {
              pld_aggr_enum = getAggregateOpEnum(groupby_operator._aggregate_expression(pld_idx-1))
            }
            if ((merg_col < num_keys) || (pld_idx <= num_plds && pld_aggr_enum != "xf::database::enums::AOP_SUM" && pld_aggr_enum != "xf::database::enums::AOP_MEAN")) {
              pld_col_selection += "1"
            } else {
              pld_col_selection += "0"
            }
          }
          cfgFuncCode += "    merge[0] = " + key_col_selection + ";"
          cfgFuncCode += "    merge[1] = 0;"
          cfgFuncCode += "    merge[2] = 0;"
          cfgFuncCode += "    merge[3] = " + pld_col_selection + ";"
          cfgFuncCode += "    merge[4] = 0;"
          var key_pld_reverse = 1
          var merged_col_reverse = 0
          cfgFuncCode += "    config[79] = (" + key_pld_reverse + ", merge[2], merge[1], merge[0]);"
          cfgFuncCode += "    config[80] = (" + merged_col_reverse + ", merge[4], merge[3]);"

          //aggr - Alec-added
          cfgFuncCode += ""
          cfgFuncCode += "    // aggr - demux mux direct_aggr"
          var nonGroupbyAggregate_operator = getNonGroupByAggregateOperator(this)
          if (nonGroupbyAggregate_operator != null) {
            cfgFuncCode += "    config[81] = 1;"
          }
          else {
            cfgFuncCode += "    config[81] = 0;"
          }

          //output - Alec-added
          cfgFuncCode += ""
          cfgFuncCode += "    // output - demux mux direct_aggr"
          cfgFuncCode += "    config[82] = 0xffff;"
          cfgFuncCode += "}"

          for (line <- cfgFuncCode) {
            _fpgaConfigFuncCode += line
          }
          if (sf == 30) {
            for (line <- cfgFuncCode_gqePart) {
              _fpgaConfigFuncCode += line
            }
          }
          var sw_aggr_consolidate_func_cal = "void " + "SW_" + nodeOpName + "_consolidate("
          if (sf == 30) {
            sw_aggr_consolidate_func_cal += "Table *" + _fpgaOutputTableName + ", "
            sw_aggr_consolidate_func_cal += "Table &" + _fpgaOutputTableName.stripSuffix("_preprocess") + ", "
            sw_aggr_consolidate_func_cal += "int hpTimes"
          } else {
            sw_aggr_consolidate_func_cal += "Table &" + _fpgaOutputTableName + ", "
            sw_aggr_consolidate_func_cal += "Table &" + _fpgaOutputTableName.stripSuffix("_preprocess")
          }

          sw_aggr_consolidate_func_cal += ") {"
          aggrConsolidateFuncCode += sw_aggr_consolidate_func_cal
          aggrConsolidateFuncCode += "    int nrow = 0;"
          var tbl_partition_suffix = ""
          if (sf == 30) {
            aggrConsolidateFuncCode += "for (int p_idx = 0; p_idx < hpTimes; p_idx++) {"
            tbl_partition_suffix = "[p_idx]"
          }
          aggrConsolidateFuncCode += "    int nrow_p = " + _fpgaOutputTableName + tbl_partition_suffix + ".getNumRow();"
          aggrConsolidateFuncCode += "    for (int r(0); r<nrow_p; ++r) {"
          var col_idx = 0
          for (o_col <- groupby_operator._outputCols) {
            var output_col_type = getColumnType(o_col, dfmap)
            // TODO: read from input table into tmp data
            var key_idx = (8-1) - groupby_operator._groupBy_operation.indexOf(o_col) // key cols are stored in a reverse order
            var pld_low_idx = 0
            var pld_high_idx = 0
            for (aggr_op <- groupby_operator._aggregate_operation) {
              if (aggr_op.split(" AS ").last == o_col) {
                var this_pld_idx = groupby_operator._aggregate_operation.indexOf(aggr_op)
                pld_low_idx = this_pld_idx // AOP::SUM[31:0] stored at 0th
                pld_high_idx = 8 + this_pld_idx // AOP::SUM[63:32] stored at 8th
              }
            }
            if (groupby_operator._groupBy_operation.contains(o_col)) { // key col
              aggrConsolidateFuncCode += "        int32_t " + stripColumnName(o_col) + " = " + _fpgaOutputTableName + tbl_partition_suffix + ".getInt32(r, " + key_idx.toString + ");"
              aggrConsolidateFuncCode += "        " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".setInt32(r, " + col_idx.toString + ", " + stripColumnName(o_col) + ");"
            }
            else { // payload col
              var pld_aggr_enum = getAggregateOpEnum(groupby_operator._aggregate_expression(pld_low_idx))
              if (pld_aggr_enum == "xf::database::enums::AOP_SUM" || pld_aggr_enum == "xf::database::enums::AOP_MEAN") {
                if (output_col_type == "IntegerType") {
                  aggrConsolidateFuncCode += "        int32_t " + stripColumnName(o_col) + " = " + _fpgaOutputTableName + tbl_partition_suffix + ".getInt32(r, " + pld_low_idx.toString + ");"
                  aggrConsolidateFuncCode += "        " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".setInt32(r, " + col_idx.toString + ", " + stripColumnName(o_col) + ");"
                }
                else if (output_col_type == "LongType") {
                  aggrConsolidateFuncCode += "        int64_t " + stripColumnName(o_col) + " = " + _fpgaOutputTableName + tbl_partition_suffix + ".combineInt64(r, " + pld_high_idx.toString + ", " + pld_low_idx.toString + ");"
                  aggrConsolidateFuncCode += "        " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".setInt64(r, " + col_idx.toString + ", " + stripColumnName(o_col) + ");"
                }
                else if (output_col_type == "DoubleType") {
                  aggrConsolidateFuncCode += "        int32_t " + stripColumnName(o_col) + " = " + _fpgaOutputTableName + tbl_partition_suffix + ".getInt32(r, " + pld_low_idx.toString + ");"
                  aggrConsolidateFuncCode += "        " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".setInt32(r, " + col_idx.toString + ", " + stripColumnName(o_col) + ");"
                }
                else {
                  aggrConsolidateFuncCode += "        // Error: unsupported data type - revisit cpu/fpga determination logic - " + output_col_type.toString + " - default to IntegerType"
                  aggrConsolidateFuncCode += "        int32_t " + stripColumnName(o_col) + " = " + _fpgaOutputTableName + tbl_partition_suffix + ".getInt32(r, " + pld_low_idx.toString + ");"
                  aggrConsolidateFuncCode += "        " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".setInt32(r, " + col_idx.toString + ", " + stripColumnName(o_col) + ");"
                }
              }
              else { // MIN, MAX, COUNT, COUNTNONZERO
                aggrConsolidateFuncCode += "        int32_t " + stripColumnName(o_col) + " = " + _fpgaOutputTableName + tbl_partition_suffix + ".getInt32(r, " + pld_low_idx.toString + ");"
                aggrConsolidateFuncCode += "        " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".setInt32(r, " + col_idx.toString + ", " + stripColumnName(o_col) + ");"
              }
            }
            col_idx += 1
          }
          aggrConsolidateFuncCode += "    }"
          aggrConsolidateFuncCode += "    nrow += nrow_p;"
          if (sf == 30) {
            aggrConsolidateFuncCode += "}"
          }
          aggrConsolidateFuncCode += "    " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".setNumRow(nrow);"
          aggrConsolidateFuncCode += "    std::cout << \"" + _fpgaOutputTableName.stripSuffix("_preprocess") + " #Row: \" << " + _fpgaOutputTableName.stripSuffix("_preprocess") + ".getNumRow() << std::endl;"
          aggrConsolidateFuncCode += "}"

          for (line <- aggrConsolidateFuncCode) {
            _fpgaSWFuncCode += line
          }

          // TODO: Set up Xilinx L2 module kernels in host code
          var kernel_name = ""
          var partition_kernel_name = ""
          if (sf == 30) {
            // if (innerMostOperator._children.head._cpuORfpga == 0 || innerMostOperator._children.head._nodeType == "SerializeFromObject") {
            // }
            partition_kernel_name = "krnl_" + nodeOpName + "_part"
            _fpgaKernelSetupCode += "AggrKrnlEngine " + partition_kernel_name + ";"
            _fpgaKernelSetupCode += partition_kernel_name + " = AggrKrnlEngine(program_a, q_a, \"gqePart\");"
            _fpgaKernelSetupCode += partition_kernel_name + ".setup_hp(512, 0, power_of_hpTimes_aggr, " + input_table_name + ", " + input_table_name + "_partition" + ", " + nodeCfgCmd_part +");"

            kernel_name = "krnl_" + nodeOpName
            _fpgaKernelSetupCode += "AggrKrnlEngine " + kernel_name + "[hpTimes_aggr];"
            _fpgaKernelSetupCode += "for (int i = 0; i < hpTimes_aggr; i++) {"
            _fpgaKernelSetupCode += "    " + kernel_name + "[i] = AggrKrnlEngine(program_a, q_a, \"gqeAggr\");"
            _fpgaKernelSetupCode += "}"
            _fpgaKernelSetupCode += "for (int i = 0; i < hpTimes_aggr; i++) {"
            _fpgaKernelSetupCode += "    " + kernel_name + "[i].setup(" + input_table_name + "_partition_array[i], " + _fpgaOutputTableName.stripSuffix("_preprocess") + "_partition_array" + "[i], " + nodeCfgCmd + ", " + nodeCfgCmd_out + ", buftmp_a);"
            _fpgaKernelSetupCode += "}"
          }
          else {
            kernel_name = "krnl_" + nodeOpName
            _fpgaKernelSetupCode += "AggrKrnlEngine " + kernel_name + ";"
            _fpgaKernelSetupCode += kernel_name + " = AggrKrnlEngine(program_a, q_a, \"gqeAggr\");"
            _fpgaKernelSetupCode += kernel_name + ".setup(" + _children.head.fpgaOutputTableName + ", " + _fpgaOutputTableName + ", " + nodeCfgCmd + ", " + nodeCfgCmd_out + ", buftmp_a);"
          }
          // TODO: Set up transfer engine in host code
          _fpgaTransEngineName = "trans_" + nodeOpName
          _fpgaTransEngineSetupCode += "transEngine " + _fpgaTransEngineName + ";"
          _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".setq(q_a);"
          if (sf == 30) {
            _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".add(&(" + nodeCfgCmd_part + "));"
          }
          _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".add(&(" + nodeCfgCmd + "));"
          for (ch <- _children) {
            if (ch.operation.isEmpty) {
              _fpgaTransEngineSetupCode += _fpgaTransEngineName + ".add(&(" + ch._fpgaOutputTableName + "));"
            }
          }
          // TODO: Set up transfer engine output
          var fpgaTransEngineOutputName = "trans_" + nodeOpName + "_out"
          // if ((parentNode == null) || ((parentNode != null) && ((parentNode.cpuORfpga == 1 && parentNode.fpgaOverlayType != this._fpgaOverlayType) || (parentNode.cpuORfpga == 0)))) {
          _fpgaTransEngineSetupCode += "transEngine " + fpgaTransEngineOutputName + ";"
          _fpgaTransEngineSetupCode += fpgaTransEngineOutputName + ".setq(q_a);"
          _fpgaTransEngineSetupCode += "q_a.finish();"
          // TODO: Set up events in host code
          var h2d_wr_name = "events_h2d_wr_" + nodeOpName
          var d2h_rd_name = "events_d2h_rd_" + nodeOpName
          var events_name = "events_" + nodeOpName
          _fpgaEventsH2DName = h2d_wr_name
          _fpgaEventsD2HName = d2h_rd_name
          _fpgaEventsName = events_name
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + h2d_wr_name + ";"
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + d2h_rd_name + ";"
          if (sf == 30) {
            _fpgaKernelEventsCode += "std::vector<cl::Event> " + events_name + "[2];"
          } else {
            _fpgaKernelEventsCode += "std::vector<cl::Event> " + events_name + ";"
          }
          _fpgaKernelEventsCode += h2d_wr_name + ".resize(1);"
          _fpgaKernelEventsCode += d2h_rd_name + ".resize(1);"
          if (sf == 30) {
            _fpgaKernelEventsCode += events_name + "[0].resize(1);"
            _fpgaKernelEventsCode += events_name + "[1].resize(hpTimes_aggr);"
          } else {
            _fpgaKernelEventsCode += events_name + ".resize(1);"
          }
          var events_grp_name = "events_grp_" + nodeOpName
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + events_grp_name + ";"
          var prev_events_grp_name = "prev_events_grp_" + nodeOpName
          _fpgaKernelEventsCode += "std::vector<cl::Event> " + prev_events_grp_name + ";"
          for (ch <- _children) {
            if (ch.operation.nonEmpty && ch._cpuORfpga == 1 && this._fpgaOverlayType == ch._fpgaOverlayType) {
              _fpgaKernelRunCode += prev_events_grp_name + ".push_back(" + ch.fpgaEventsH2DName + "[0]);"
            }
          }
          // TODO: Run Xilinx L2 module kernels and link events in host code
          for (ch <- _children) {
            if (ch._operation.nonEmpty) {
              _fpgaKernelRunCode += _fpgaTransEngineName + ".add(&(" + ch._fpgaOutputTableName + "));"
            }
          }
          _fpgaKernelRunCode += _fpgaTransEngineName + ".host2dev(0, &(" + prev_events_grp_name + "), &(" + _fpgaEventsH2DName + "[0]));"
          _fpgaKernelRunCode += events_grp_name + ".push_back(" + _fpgaEventsH2DName + "[0]);"
          for (ch <- _children) {
            if (ch.operation.nonEmpty && ch._cpuORfpga == 1 && this._fpgaOverlayType == ch._fpgaOverlayType) {
              if (sf == 30) {
                _fpgaKernelRunCode += events_grp_name + ".insert(std::end(" + events_grp_name + "), std::begin(" + ch.fpgaEventsName + "[0]), std::end(" + ch.fpgaEventsName + "[0]));"
                _fpgaKernelRunCode += events_grp_name + ".insert(std::end(" + events_grp_name + "), std::begin(" + ch.fpgaEventsName + "[1]), std::end(" + ch.fpgaEventsName + "[1]));"
              } else {
                _fpgaKernelRunCode += events_grp_name + ".push_back(" + ch.fpgaEventsName + "[0]);"
              }
            }
          }
          if (sf == 30) {
            _fpgaKernelRunCode += partition_kernel_name + ".run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[0][0]));"
            _fpgaKernelRunCode += "for (int i(0); i < hpTimes_aggr; ++i) {"
            _fpgaKernelRunCode += "    " + kernel_name + "[i].run(0, &(" + _fpgaEventsName + "[0]), &(" + _fpgaEventsName + "[1][i]));"
            _fpgaKernelRunCode += "}"
          } else {
            _fpgaKernelRunCode += kernel_name + ".run(0, &(" + events_grp_name + "), &(" + _fpgaEventsName + "[0]));"
          }
          _fpgaKernelRunCode += ""
          if (sf == 30) {
            _fpgaKernelRunCode += "for (int i(0); i < hpTimes_aggr; ++i) {"
            _fpgaKernelRunCode += "    " + fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName.stripSuffix("_preprocess") + "_partition_array" + "[i]));"
            _fpgaKernelRunCode += "}"
            _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "[1]), &(" + _fpgaEventsD2HName + "[0]));"
          } else {
            _fpgaKernelRunCode += fpgaTransEngineOutputName + ".add(&(" + _fpgaOutputTableName + "));"
            _fpgaKernelRunCode += fpgaTransEngineOutputName + ".dev2host(0, &(" + _fpgaEventsName + "), &(" + _fpgaEventsD2HName + "[0]));"
          }
          _fpgaKernelRunCode += "q_a.flush();"
          _fpgaKernelRunCode += "q_a.finish();"
          _fpgaKernelRunCode += ""
          var tmp_aggr_col_consolidate = "SW_" + nodeOpName + "_consolidate("
          if (sf == 30) {
            tmp_aggr_col_consolidate += _fpgaOutputTableName.stripSuffix("_preprocess")  + "_partition_array" + ", "
          } else {
            tmp_aggr_col_consolidate += _fpgaOutputTableName + ", "
          }
          _fpgaOutputTableName = _fpgaOutputTableName.stripSuffix("_preprocess")
          if (sf == 30) {
            tmp_aggr_col_consolidate += _fpgaOutputTableName + ", hpTimes_aggr);"
          } else {
            tmp_aggr_col_consolidate += _fpgaOutputTableName + ");"
          }
          _fpgaKernelRunCode += tmp_aggr_col_consolidate
        }
        else if (_fpgaOverlayType == 2) { // gqe_join-0, gqe_aggr-1, gqe_part-2
          var nodeCfgCmd = "cfg_" + nodeOpName + "_cmds"
          var nodeGetCfgFuncName = "get_cfg_dat_" + nodeOpName + "_gqe_part"
          _fpgaConfigCode += "cfgCmd " + nodeCfgCmd + ";"
          _fpgaConfigCode += nodeCfgCmd + ".allocateHost();"
          _fpgaConfigCode += nodeGetCfgFuncName + " (" + nodeCfgCmd + ".cmd);"
          if (_fpgaOverlayType == 0) {
            _fpgaConfigCode += nodeCfgCmd + ".allocateDevBuffer(context_h, 32);"
          }
          else if (_fpgaOverlayType == 1) {
            _fpgaConfigCode += nodeCfgCmd + ".allocateDevBuffer(context_a, 32);"
          }

          // Generate the function that generates the Xilinx L2 module configurations
          var cfgFuncCode = new ListBuffer[String]()
          cfgFuncCode += "void " + nodeGetCfgFuncName + "(ap_uint<512>* hbuf) {"
          var input_table_col = _children.head.outputCols
          // ----------------------------------- Debug info -----------------------------------
          cfgFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
          cfgFuncCode += "    // Supported operation: " + _nodeType
          cfgFuncCode += "    // Operation: " + _operation
          cfgFuncCode += "    // Input Table: " + input_table_col
          cfgFuncCode += "    // Output Table: " + _outputCols
          cfgFuncCode += "    // Node Depth: " + _treeDepth
          cfgFuncCode += "    ap_uint<512>* b = hbuf;"
          cfgFuncCode += "    memset(b, 0, sizeof(ap_uint<512>) * 9);"
          cfgFuncCode += "    ap_uint<512> t = 0;"
          cfgFuncCode += "    t.set_bit(2, 0); // dual-key - not sure what this does, yet" + "\n"

          _nodeType match {
            case "Filter" =>
              //              enum FilterOp {
              //                FOP_EQ,     ///< equal
              //                FOP_DC = 0, ///< don't care, always true.
              //                FOP_NE,     ///< not equal
              //                FOP_GT,     ///< greater than, signed.
              //                FOP_LT,     ///< less than, signed.
              //                FOP_GE,     ///< greater than or equal, signed.
              //                FOP_LE,     ///< less than or equal, signed.
              //                FOP_GTU,    ///< greater than, unsigned.
              //                FOP_LTU,    ///< less than, unsigned.
              //                FOP_GEU,    ///< greater than or equal, unsigned.
              //                FOP_LEU     ///< less than or equal, unsigned.
              //              };
              var filterCfgFuncCode = new ListBuffer[String]()
              var filterCfgFuncName = "gen_fcfg_" + _fpgaNodeName
              filterCfgFuncCode += "static void " + filterCfgFuncName + "(uint32_t cfg[]) {"
              filterCfgFuncCode += "    using namespace xf::database;"
              filterCfgFuncCode += "    int n = 0;"

              // filterConditions_const => Map(col_idx, [filter_val, filter_op])
              val filterConditions_const = collection.mutable.Map[Int, ListBuffer[(String, String)]]()
              val filterConditions_col = collection.mutable.Map[(Int, Int), String]()
              var filter_clauses = _operation.head.stripPrefix("(").stripSuffix(")").split(" AND ")
              for (clause <- filter_clauses) {
                if (!clause.contains("isnotnull")) {
                  var clause_formatted = clause
                  while (clause_formatted.contains("(") | clause_formatted.contains(")")) {
                    clause_formatted = clause_formatted.replace("(", "")
                    clause_formatted = clause_formatted.replace(")", "")
                  }
                  var col_name = clause_formatted.split(" ").head
                  var col_val = clause_formatted.split(" ").last
                  var filter_clause_col_idx = input_table_col.indexOf(col_name)
                  var filter_clause_val_idx = input_table_col.indexOf(col_val)

                  var col_op = "FOP_DC"
                  if (clause_formatted.contains(" = ")) {
                    col_op = "FOP_EQ"
                  } else if (clause_formatted.contains(" != ")) {
                    col_op = "FOP_NE"
                  } else if (clause_formatted.contains(" > ")) {
                    col_op = "FOP_GTU"
                  } else if (clause_formatted.contains(" < ")) {
                    col_op = "FOP_LTU"
                  } else if (clause_formatted.contains(" >= ")) {
                    col_op = "FOP_GEU"
                  } else if (clause_formatted.contains(" <= ")) {
                    col_op = "FOP_LEU"
                  } else {
                    col_op = "FOP_DC"
                  }

                  if (filter_clause_val_idx == -1) { //const filtering factor
                    // e.g. columnDictionary += (col -> (tcph_table, col_first))
                    if (filterConditions_const.contains(filter_clause_col_idx)) {
                      var filterValOp = filterConditions_const(filter_clause_col_idx)
                      var temp = (col_val, col_op)
                      filterValOp += temp
                      filterConditions_const += (filter_clause_col_idx -> filterValOp)
                    } else {
                      var filterValOp = new ListBuffer[(String, String)]()
                      var temp = (col_val, col_op)
                      filterValOp += temp
                      filterConditions_const += (filter_clause_col_idx -> filterValOp)
                    }
                  } else {
                    if (filter_clause_col_idx > filter_clause_val_idx) {
                      val temp = (filter_clause_val_idx, filter_clause_col_idx)
                      if (col_op == "FOP_GTU") {
                        col_op = "FOP_LTU"
                      } else if (col_op == "FOP_LTU") {
                        col_op = "FOP_GTU"
                      } else if (col_op == "FOP_GEU") {
                        col_op = "FOP_LEU"
                      } else if (col_op == "FOP_LEU") {
                        col_op = "FOP_GEU"
                      }
                      filterConditions_col += (temp -> col_op)
                    } else {
                      val temp = (filter_clause_col_idx, filter_clause_val_idx)
                      filterConditions_col += (temp -> col_op)
                    }
                  }
                }
              }
              println(filterConditions_const)
              // check if map col is <= 4 and the number of filter condition is <= 2
              var num_col = filterConditions_const.size
              if (num_col > 4) {
                filterCfgFuncCode += "//Unsupported number of filter columns (num_col > 4)"
              }
              for ((key, payload) <- filterConditions_const) {
                if (payload.length > 2) {
                  filterCfgFuncCode += "//Unsupported number of filter conditions (num_col > 2)"
                }
              }

              var i = 0
              for (i <- 0 to 4 - 1) {
                filterCfgFuncCode += "    // cond_" + (i + 1).toString
                if (filterConditions_const.contains(i)) {
                  var payloadValOp = filterConditions_const(i)
                  if (payloadValOp.length == 1) {
                    filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
                    filterCfgFuncCode += "    cfg[n++] = (uint32_t)" + payloadValOp.head._1 + "UL;"
                    filterCfgFuncCode += "    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (" + payloadValOp.head._2 + ");"
                  } else if (payloadValOp.length == 2) {
                    filterCfgFuncCode += "    cfg[n++] = (uint32_t)" + payloadValOp.head._1 + "UL;"
                    filterCfgFuncCode += "    cfg[n++] = (uint32_t)" + payloadValOp.last._1 + "UL;"
                    filterCfgFuncCode += "    cfg[n++] = 0UL | (" + payloadValOp.head._2 + " << FilterOpWidth) | (" + payloadValOp.last._2 + ");"
                  } else {
                    filterCfgFuncCode += "    //Unsupported Op"
                  }
                } else {
                  filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
                  filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
                  filterCfgFuncCode += "    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (FOP_DC);"
                }
              }
              filterCfgFuncCode += "    "
              filterCfgFuncCode += "    uint32_t r = 0;"
              filterCfgFuncCode += "    int sh = 0;"
              var j = 0
              i = 0
              for (i <- 0 to 2) {
                for (j <- i + 1 to 3) {
                  if (filterConditions_col.contains((i, j))) {
                    filterCfgFuncCode += "    r |= ((uint32_t)(" + filterConditions_col(i, j) + " << sh));"
                    filterCfgFuncCode += "    sh += FilterOpWidth;"
                  } else {
                    filterCfgFuncCode += "    r |= ((uint32_t)(FOP_DC << sh));"
                    filterCfgFuncCode += "    sh += FilterOpWidth;"
                  }
                }
              }
              filterCfgFuncCode += "    cfg[n++] = r;" + "\n"
              filterCfgFuncCode += "    // 4 true and 6 true"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)0UL;"
              filterCfgFuncCode += "    cfg[n++] = (uint32_t)(1UL << 31);"
              filterCfgFuncCode += "}"

              // Print out filterCfgFuncCode first
              for (line <- filterCfgFuncCode) {
                _fpgaConfigFuncCode += line
              }

              cfgFuncCode += "    //input table col indices"
              var input_tbl_col_list = "signed char id[] = {"
              var a = 0
              var num_input_tbl_col = input_table_col.length
              for (a <- 0 to 8 - 1) {
                if (a < num_input_tbl_col) {
                  input_tbl_col_list += a.toString + ", "
                } else {
                  input_tbl_col_list += "-1, "
                }
              }
              input_tbl_col_list = input_tbl_col_list.stripSuffix(", ")
              input_tbl_col_list += "};"
              cfgFuncCode += "    " + input_tbl_col_list
              cfgFuncCode += "    for (int c = 0; c < 8; ++c) {"
              cfgFuncCode += "        t.range(56 + 8 * c + 7, 56 + 8 * c) = id[c];"
              cfgFuncCode += "    }"

              cfgFuncCode += "    b[0] = t;" + "\n"

              cfgFuncCode += "    //filter"
              cfgFuncCode += "    uint32_t cfg[45];"
              cfgFuncCode += "    " + filterCfgFuncName + "(cfg);"
              cfgFuncCode += "    memcpy(&b[3], cfg, sizeof(uint32_t) * 45);" + "\n"
            case _ =>
              cfgFuncCode += "    // Unsupported operation: " + _nodeType
          }
          cfgFuncCode += "}"
          for (line <- cfgFuncCode) {
            _fpgaConfigFuncCode += line
          }
          // TODO: Set up Xilinx L2 module kernels in host code
          // TODO: Set up Xilinx L2 module kernels
          var kernel_name = "krnl_" + nodeOpName
          // TODO: implement logic to decide which type of overlay to use: krnlEngine (gqe_join) or AggrKrnlEngine (gqe_aggr)
          if (_fpgaOverlayType == 0) {
            _fpgaKernelSetupCode += "krnlEngine " + kernel_name + ";"
            _fpgaKernelSetupCode += kernel_name + " = krnlEngine(program_h, q_h, \"gqePart\");"
          }
          else if (_fpgaOverlayType == 1) {
            _fpgaKernelSetupCode += "AggrKrnlEngine " + kernel_name + ";"
            _fpgaKernelSetupCode += kernel_name + " = AggrKrnlEngine(program_a, q_a, \"gqePart\");"
          }

          _fpgaConfigCode += "int psize = 16;" // Default value
          _fpgaConfigCode += "int hjRow = 1 << psize;" // TODO: implement logic to automatically decide partition size
          _fpgaConfigCode += "int hpTimes = 1 << (int)ceil(log2((float)(" + _children.head.fpgaOutputTableName + ".nrow / hjRow)));"
          _fpgaConfigCode += "int power_of_hpTimes = log2(hpTimes);"
          _fpgaConfigCode += "std::cout << \"Number of partition is: \" << hpTimes << std::endl;"
          var fpgaOutputTableName_partitioned = _fpgaOutputTableName + "_partitioned"
          _fpgaKernelSetupCode += kernel_name + ".setup_hp( 512, 0, power_of_hpTimes, " + _children.head.fpgaOutputTableName + ", " +  fpgaOutputTableName_partitioned + ", " + nodeCfgCmd + ");"
          // TODO: Set up transfer engine
          // TODO: Set up events
          // TODO: Run Xilinx L2 module kernels and link events
        }
        else {
          println("*******************************************************")
          println("********Error: Unsupported FPGA overlay type***********")
          println("*******************************************************")
        }
      }
      else { // Execute on CPU
        // tag:sw
        _fpgaSWFuncName = "SW_" + nodeOpName; // Generate SW implementations of the operation
        var swFuncCall = _fpgaSWFuncName + "("
        var contains_partitioned_tbl = false
        var overlay_type = -1 // 0 - gqeJoin, 1 - gqeAggr
        for (ch <- _children){
          if (sf == 30) {
            if (ch._cpuORfpga == 1 && ch._nodeType != "SerializeFromObject") {
              contains_partitioned_tbl = true
              overlay_type = ch._fpgaOverlayType
              if (overlay_type == 1) {
                swFuncCall += ch.fpgaOutputTableName + ", "
              } else {
                swFuncCall += ch.fpgaOutputTableName + "_partition_array, "
              }
            } else {
              swFuncCall += ch.fpgaOutputTableName + ", "
            }
          } else {
            swFuncCall += ch.fpgaOutputTableName + ", "
          }
        }
        if (_stringRowIDBackSubstitution == true) {
          var orig_table_names = get_stringRowIDOriginalTableName(this)
          for (orig_tbl <- orig_table_names){
            swFuncCall += orig_tbl + ", "
          }
        }
        if (sf == 30) {
          if (contains_partitioned_tbl == true) {
            if (overlay_type == 0) {
              swFuncCall += _fpgaOutputTableName + ", hpTimes_join);"
            } else if (overlay_type == 1) {
              // swFuncCall += _fpgaOutputTableName + ", hpTimes_aggr);"
              swFuncCall += _fpgaOutputTableName + ");"
            } else {
              swFuncCall += _fpgaOutputTableName + ", ERROR);"
            }
          } else {
            swFuncCall += _fpgaOutputTableName + ");"
          }
        } else {
          swFuncCall += _fpgaOutputTableName + ");"
        }
        _fpgaSWCode += swFuncCall
        // SW Function Code
        _nodeType match {
          case "JOIN_INNER" =>
            // tag:innerjoin
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children) {
              if (sf == 30 && ch._cpuORfpga == 1 && ch._nodeType != "SerializeFromObject") {
                tempStr += "Table *" + ch.fpgaOutputTableName + ", "
              } else {
                tempStr += "Table &" + ch.fpgaOutputTableName + ", "
              }
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            if (sf == 30 && ((_children.head._cpuORfpga == 1 && _children.head._nodeType != "SerializeFromObject") || (_children.last._cpuORfpga == 1 && _children.last._nodeType != "SerializeFromObject"))) {
              tempStr += "Table &" + _fpgaOutputTableName + ", int hpTimes) {"
            } else {
              tempStr += "Table &" + _fpgaOutputTableName + ") {"
            }
            var tbl_in_1 = _children.head.fpgaOutputTableName
            var tbl_in_2 = _children.last.fpgaOutputTableName
            var join_left_table_col = _children.head.outputCols
            var join_right_table_col = _children.last.outputCols

            println("------Join Inner Terms: " + _operation.head)
            var (structCode_leftMajor, leftTableKeyColNames_leftMajor, rightTableKeyColNames_leftMajor, leftTablePayloadColNames_leftMajor, rightTablePayloadColNames_leftMajor, joinKeyTypeName_leftMajor, joinPayloadTypeName_leftMajor) = genJoin_KeyPayloadStruct(this, "leftMajor", dfmap);
            var (structCode_rightMajor, leftTableKeyColNames_rightMajor, rightTableKeyColNames_rightMajor, leftTablePayloadColNames_rightMajor, rightTablePayloadColNames_rightMajor, joinKeyTypeName_rightMajor, joinPayloadTypeName_rightMajor) = genJoin_KeyPayloadStruct(this, "rightMajor", dfmap);
            for (line <- structCode_leftMajor) {
              _fpgaSWFuncCode += line
            }
            for (line <- structCode_rightMajor) {
              _fpgaSWFuncCode += line
            }

            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            // ----------------------------------- Debug info -----------------------------------
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Left Table: " + join_left_table_col
            _fpgaSWFuncCode += "    // Right Table: " + join_right_table_col
            _fpgaSWFuncCode += "    // Output Table: " + _outputCols
            // ----------------------------------- Debug info -----------------------------------
            if (sf == 30 && _children.head._cpuORfpga == 1 && _children.head._nodeType != "SerializeFromObject") {
              _fpgaSWFuncCode += "    int left_nrow = " + tbl_in_1 + "[0].getNumRow();"
            } else {
              _fpgaSWFuncCode += "    int left_nrow = " + tbl_in_1 + ".getNumRow();"
            }
            if (sf == 30 && _children.last._cpuORfpga == 1 && _children.last._nodeType != "SerializeFromObject") {
              _fpgaSWFuncCode += "    int right_nrow = " + tbl_in_2 + "[0].getNumRow();"
            } else {
              _fpgaSWFuncCode += "    int right_nrow = " + tbl_in_2 + ".getNumRow();"
            }
            _fpgaSWFuncCode += "    if (left_nrow < right_nrow) { "
            var innerJoin_leftMajor_code = genInnerJoin_core(this, "leftMajor", dfmap, leftTableKeyColNames_leftMajor, rightTableKeyColNames_leftMajor, leftTablePayloadColNames_leftMajor, rightTablePayloadColNames_leftMajor, joinKeyTypeName_leftMajor, joinPayloadTypeName_leftMajor, sf)
            for (line <- innerJoin_leftMajor_code) {
              _fpgaSWFuncCode += "    " + line
            }
            _fpgaSWFuncCode += "    } else { "
            var innerJoin_rightMajor_code = genInnerJoin_core(this, "rightMajor", dfmap, leftTableKeyColNames_rightMajor, rightTableKeyColNames_rightMajor, leftTablePayloadColNames_rightMajor, rightTablePayloadColNames_rightMajor, joinKeyTypeName_rightMajor, joinPayloadTypeName_rightMajor, sf)
            for (line <- innerJoin_rightMajor_code) {
              _fpgaSWFuncCode += "    " + line
            }
            _fpgaSWFuncCode += "    } "
          case "JOIN_LEFTANTI" =>
            // tag:antijoin
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children) {
              tempStr += "Table &" + ch.fpgaOutputTableName + ", "
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            tempStr += "Table &" + _fpgaOutputTableName + ") {"

            var tbl_in_1 = _children.head.fpgaOutputTableName
            var tbl_in_2 = _children.last.fpgaOutputTableName
            var join_left_table_col = _children.head.outputCols
            var join_right_table_col = _children.last.outputCols

            println("------Join LEFTANTI Terms: " + _operation.head)
            var (structCode_leftMajor, leftTableKeyColNames_leftMajor, rightTableKeyColNames_leftMajor, leftTablePayloadColNames_leftMajor, rightTablePayloadColNames_leftMajor, joinKeyTypeName_leftMajor, joinPayloadTypeName_leftMajor) = genJoin_KeyPayloadStruct(this, "leftMajor", dfmap);
            var (structCode_rightMajor, leftTableKeyColNames_rightMajor, rightTableKeyColNames_rightMajor, leftTablePayloadColNames_rightMajor, rightTablePayloadColNames_rightMajor, joinKeyTypeName_rightMajor, joinPayloadTypeName_rightMajor) = genJoin_KeyPayloadStruct(this, "rightMajor", dfmap);
            for (line <- structCode_leftMajor) {
              _fpgaSWFuncCode += line
            }
            for (line <- structCode_rightMajor) {
              _fpgaSWFuncCode += line
            }

            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            // ----------------------------------- Debug info -----------------------------------
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Left Table: " + join_left_table_col
            _fpgaSWFuncCode += "    // Right Table: " + join_right_table_col
            _fpgaSWFuncCode += "    // Output Table: " + _outputCols
            // ----------------------------------- Debug info -----------------------------------
            _fpgaSWFuncCode += "    int left_nrow = " + tbl_in_1 + ".getNumRow();"
            _fpgaSWFuncCode += "    int right_nrow = " + tbl_in_2 + ".getNumRow();"
            var antiJoin_rightMajor_code = genAntiJoin_rightMajor_core(this, dfmap, leftTableKeyColNames_rightMajor, rightTableKeyColNames_rightMajor, leftTablePayloadColNames_rightMajor, rightTablePayloadColNames_rightMajor, joinKeyTypeName_rightMajor, joinPayloadTypeName_rightMajor)
            for (line <- antiJoin_rightMajor_code) {
              _fpgaSWFuncCode += "    " + line
            }
          case "JOIN_LEFTSEMI" =>
            // tag:semijoin
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children) {
              tempStr += "Table &" + ch.fpgaOutputTableName + ", "
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            tempStr += "Table &" + _fpgaOutputTableName + ") {"

            var tbl_in_1 = _children.head.fpgaOutputTableName
            var tbl_in_2 = _children.last.fpgaOutputTableName
            var join_left_table_col = _children.head.outputCols
            var join_right_table_col = _children.last.outputCols

            println("------Join LEFTSEMI Terms: " + _operation.head)
            var (structCode_leftMajor, leftTableKeyColNames_leftMajor, rightTableKeyColNames_leftMajor, leftTablePayloadColNames_leftMajor, rightTablePayloadColNames_leftMajor, joinKeyTypeName_leftMajor, joinPayloadTypeName_leftMajor) = genJoin_KeyPayloadStruct(this, "leftMajor", dfmap);
            var (structCode_rightMajor, leftTableKeyColNames_rightMajor, rightTableKeyColNames_rightMajor, leftTablePayloadColNames_rightMajor, rightTablePayloadColNames_rightMajor, joinKeyTypeName_rightMajor, joinPayloadTypeName_rightMajor) = genJoin_KeyPayloadStruct(this, "rightMajor", dfmap);
            for (line <- structCode_leftMajor) {
              _fpgaSWFuncCode += line
            }
            for (line <- structCode_rightMajor) {
              _fpgaSWFuncCode += line
            }

            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            // ----------------------------------- Debug info -----------------------------------
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Left Table: " + join_left_table_col
            _fpgaSWFuncCode += "    // Right Table: " + join_right_table_col
            _fpgaSWFuncCode += "    // Output Table: " + _outputCols
            // ----------------------------------- Debug info -----------------------------------
            _fpgaSWFuncCode += "    int left_nrow = " + tbl_in_1 + ".getNumRow();"
            _fpgaSWFuncCode += "    int right_nrow = " + tbl_in_2 + ".getNumRow();"

            // The section below includes both implementation version of leftsemi_join(...); however hash.erase() might be causing segmentation fault
            //              var join_clauses = getJoinKeyTerms(this.joining_expression(0), false)
            //              var isSpecialSemiJoin = join_clauses.length == 2 && ((join_clauses(0).contains("!=") && !join_clauses(1).contains("!=")) || (!join_clauses(0).contains("!=") && join_clauses(1).contains("!=")))
            //              _fpgaSWFuncCode += "    if (left_nrow < right_nrow" + " && (" + !isSpecialSemiJoin + ")" + ") { "
            //              var semiJoin_leftMajor_code = genSemiJoin_leftMajor_core(this, dfmap, leftTableKeyColNames_leftMajor, rightTableKeyColNames_leftMajor, leftTablePayloadColNames_leftMajor, rightTablePayloadColNames_leftMajor, joinKeyTypeName_leftMajor, joinPayloadTypeName_leftMajor)
            //              for (line <- semiJoin_leftMajor_code) {
            //                _fpgaSWFuncCode += "    " + line
            //              }
            //              _fpgaSWFuncCode += "    } else { "
            //              var semiJoin_rightMajor_code = genSemiJoin_rightMajor_core(this, dfmap, leftTableKeyColNames_rightMajor, rightTableKeyColNames_rightMajor, leftTablePayloadColNames_rightMajor, rightTablePayloadColNames_rightMajor, joinKeyTypeName_rightMajor, joinPayloadTypeName_rightMajor)
            //              for (line <- semiJoin_rightMajor_code) {
            //                _fpgaSWFuncCode += "    " + line
            //              }
            //              _fpgaSWFuncCode += "    } "

            // This one keeps the original implementation of LEFTSEMI_JOIN(...)
            var semiJoin_rightMajor_code = genSemiJoin_rightMajor_core(this, dfmap, leftTableKeyColNames_rightMajor, rightTableKeyColNames_rightMajor, leftTablePayloadColNames_rightMajor, rightTablePayloadColNames_rightMajor, joinKeyTypeName_rightMajor, joinPayloadTypeName_rightMajor)
            for (line <- semiJoin_rightMajor_code) {
              _fpgaSWFuncCode += "    " + line
            }
          case "JOIN_LEFTOUTER" =>
            // tag:outerjoin
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children) {
              tempStr += "Table &" + ch.fpgaOutputTableName + ", "
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            tempStr += "Table &" + _fpgaOutputTableName + ") {"

            var tbl_in_1 = _children.last.fpgaOutputTableName
            var tbl_in_2 = _children.head.fpgaOutputTableName
            var join_left_table_col = _children.last.outputCols
            var join_right_table_col = _children.head.outputCols
            var tbl_out_1 = _fpgaOutputTableName

            println("------Join LEFTOUTER Terms: " + _operation.head)
            var join_pairs = getJoinTerms(joining_expression(0))
            var join_key_pairs = getJoinKeyTerms(joining_expression(0), false)
            var join_filter_pairs = getJoinFilterTerms(joining_expression(0), false)
            var num_join_key_pairs = join_key_pairs.length

            //Key struct
            var leftTableKeyColNames = new ListBuffer[String]()
            var rightTableKeyColNames = new ListBuffer[String]()
            var joinKeyTypeName = _fpgaSWFuncName + "_key"
            _fpgaSWFuncCode += "struct " + joinKeyTypeName + " {"

            println("# Key Operations: " + num_join_key_pairs)

            for (key_pair <- join_key_pairs) {
              println(key_pair)
              // var leftTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" = ").head
              // var rightTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" = ").last
              var leftTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" ").head
              var rightTblCol = key_pair.stripPrefix("(").stripSuffix(")").trim.split(" ").last
              if (join_left_table_col.indexOf(leftTblCol) == -1) {
                var tmpTblCol = leftTblCol
                leftTblCol = rightTblCol
                rightTblCol = tmpTblCol
              }
              leftTableKeyColNames += leftTblCol
              rightTableKeyColNames += rightTblCol
              var leftTblColType = getColumnType(leftTblCol, dfmap)
              var leftTblColName = stripColumnName(leftTblCol)
              leftTblColType match {
                case "IntegerType" =>
                  _fpgaSWFuncCode += "    int32_t " + leftTblColName + ";"
                case "LongType" =>
                  _fpgaSWFuncCode += "    int64_t " + leftTblColName + ";"
                case "StringType" =>
                  _fpgaSWFuncCode += "    std::string " + leftTblColName + ";"
                case _ =>
                  _fpgaSWFuncCode += "    // Unsupported join key type"
              }
            }
            _fpgaSWFuncCode += "    bool operator==(const " + joinKeyTypeName + "& other) const {"
            var equivalentOperation = "        return ("
            var i = 0
            for (i <- 0 to leftTableKeyColNames.length - 2) {
              var equality = join_key_pairs(i).stripPrefix("(").stripSuffix(")").trim.split(" ")(1) // operator is in index 1
              if (equality == "=") {
                equivalentOperation += "(" + stripColumnName(leftTableKeyColNames(i)) + " == other." + stripColumnName(leftTableKeyColNames(i)) + ") && "
              } else if (equality == "!=") {
                equivalentOperation += "(" + stripColumnName(leftTableKeyColNames(i)) + " != other." + stripColumnName(leftTableKeyColNames(i)) + ") && "
              }
            }
            var equality = join_key_pairs.last.stripPrefix("(").stripSuffix(")").trim.split(" ")(1) // operator is in index 1
            if (equality == "=") {
              equivalentOperation += "(" + stripColumnName(leftTableKeyColNames.last) + " == other." + stripColumnName(leftTableKeyColNames.last) + "));"
            } else if (equality == "!=") {
              equivalentOperation += "(" + stripColumnName(leftTableKeyColNames.last) + " != other." + stripColumnName(leftTableKeyColNames.last) + "));"
            }
            _fpgaSWFuncCode += equivalentOperation
            _fpgaSWFuncCode += "    }"
            _fpgaSWFuncCode += "};"

            //Key hash struct - joinKeyHashTypeName
            _fpgaSWFuncCode += "namespace std {"
            _fpgaSWFuncCode += "template <>"
            _fpgaSWFuncCode += "struct hash<" + joinKeyTypeName + "> {"
            _fpgaSWFuncCode += "    std::size_t operator() (const " + joinKeyTypeName + "& k) const {"
            _fpgaSWFuncCode += "        using std::size_t;"
            _fpgaSWFuncCode += "        using std::hash;"
            _fpgaSWFuncCode += "        using std::string;"
            var joinKeyHashStr = "        return "
            i = 0
            for (left_key_pair <- leftTableKeyColNames) {
              var equality = join_key_pairs(i).stripPrefix("(").stripSuffix(")").trim.split(" ")(1) // operator is in index 1
              if (equality == "=") {
                var leftTblColType = getColumnType(left_key_pair, dfmap)
                var leftTblColName = stripColumnName(left_key_pair)
                leftTblColType match {
                  case "IntegerType" =>
                    joinKeyHashStr += "(hash<int32_t>()(k." + leftTblColName + ")) + "
                  case "LongType" =>
                    joinKeyHashStr += "(hash<int64_t>()(k." + leftTblColName + ")) + "
                  case "StringType" =>
                    joinKeyHashStr += "(hash<string>()(k." + leftTblColName + ")) + "
                  case _ =>
                    _fpgaSWFuncCode += "        // Unsupported join key type"
                }
              }
              i += 1
            }
            _fpgaSWFuncCode += joinKeyHashStr.stripSuffix(" + ") + ";"
            _fpgaSWFuncCode += "    }"
            _fpgaSWFuncCode += "};"
            _fpgaSWFuncCode += "}"

            //Payload struct - Left table
            var leftTablePayloadColNames = new ListBuffer[String]()
            var joinPayloadTypeName = _fpgaSWFuncName + "_payload"
            _fpgaSWFuncCode += "struct " + joinPayloadTypeName + " {"
            for (left_payload <- join_left_table_col) {
              var leftTblColName = stripColumnName(left_payload)
              var leftPayloadColType = getColumnType(left_payload, dfmap)
              leftPayloadColType match {
                case "IntegerType" =>
                  _fpgaSWFuncCode += "    int32_t " + leftTblColName + ";"
                case "LongType" =>
                  _fpgaSWFuncCode += "    int64_t " + leftTblColName + ";"
                case "StringType" =>
                  _fpgaSWFuncCode += "    std::string " + leftTblColName + ";"
                case _ =>
                  _fpgaSWFuncCode += "    // Unsupported join key type"
              }
              leftTablePayloadColNames += left_payload
            }
            _fpgaSWFuncCode += "};"
            //Payload struct - Right table
            var rightTablePayloadColNames = new ListBuffer[String]()
            for (right_payload <- join_right_table_col) {
              rightTablePayloadColNames += right_payload
            }
            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            // ----------------------------------- Debug info -----------------------------------
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Left Table: " + join_right_table_col
            _fpgaSWFuncCode += "    // Right Table: " + join_left_table_col
            _fpgaSWFuncCode += "    // Output Table: " + _outputCols
            // ----------------------------------- Debug info -----------------------------------
            _fpgaSWFuncCode += "    std::unordered_multimap<" + joinKeyTypeName + ", " + joinPayloadTypeName + "> ht1;"
            _fpgaSWFuncCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
            _fpgaSWFuncCode += "    int nrow2 = " + tbl_in_2 + ".getNumRow();"
            // Enumerate input table
            _fpgaSWFuncCode += "    for (int i = 0; i < nrow1; i++) {"
            //  Key
            var key_str = ""
            for (key_col <- leftTableKeyColNames) {
              var join_left_key_col_name = stripColumnName(key_col) + "_k"
              var join_left_key_col_type = getColumnType(key_col, dfmap)
              var join_left_key_col_idx = join_left_table_col.indexOf(key_col)
              join_left_key_col_type match {
                case "IntegerType" =>
                  _fpgaSWFuncCode += "        int32_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_key_col_idx + ");"
                case "LongType" =>
                  _fpgaSWFuncCode += "        int64_t " + join_left_key_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_key_col_idx + ");"
                case "StringType" =>
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_left_key_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_left_key_col_idx + ");"
                  _fpgaSWFuncCode += "        std::string " + join_left_key_col_name + " = std::string(" + join_left_key_col_name + "_n.data());"
                case _ =>
                  _fpgaSWFuncCode += "        // Unsupported join key type"
              }
              key_str += join_left_key_col_name + ", "
            }
            _fpgaSWFuncCode += "        " + joinKeyTypeName + " keyA{" + key_str.stripSuffix(", ") + "};"
            //  Payload
            var payload_str = ""
            for (payload_col <- leftTablePayloadColNames) {
              var join_left_payload_col_name = stripColumnName(payload_col)
              var join_left_payload_col_type = getColumnType(payload_col, dfmap)
              var join_left_payload_col_idx = join_left_table_col.indexOf(payload_col)
              join_left_payload_col_type match {
                case "IntegerType" =>
                  _fpgaSWFuncCode += "        int32_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt32(i, " + join_left_payload_col_idx + ");"
                case "LongType" =>
                  _fpgaSWFuncCode += "        int64_t " + join_left_payload_col_name + " = " + tbl_in_1 + ".getInt64(i, " + join_left_payload_col_idx + ");"
                case "StringType" =>
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1> " + join_left_payload_col_name + "_n = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(payload_col)) + " + 1>(i, " + join_left_payload_col_idx + ");"
                  _fpgaSWFuncCode += "        std::string " + join_left_payload_col_name + " = std::string(" + join_left_payload_col_name + "_n.data());"
                case _ =>
                  _fpgaSWFuncCode += "        // Unsupported join key type"
              }
              payload_str += join_left_payload_col_name + ", "
            }
            _fpgaSWFuncCode += "        " + joinPayloadTypeName + " payloadA{" + payload_str.stripSuffix(", ") + "};"
            _fpgaSWFuncCode += "        ht1.insert(std::make_pair(keyA, payloadA));"
            _fpgaSWFuncCode += "    }"
            _fpgaSWFuncCode += "    int r = 0;"
            // Enumerate output table
            _fpgaSWFuncCode += "    for (int i = 0; i < nrow2; i++) {"
            //  Key
            key_str = ""
            for (key_col <- rightTableKeyColNames) {
              var join_right_key_col_name = stripColumnName(key_col) + "_k"
              var join_right_key_col_type = getColumnType(key_col, dfmap)
              var join_right_key_col_idx = join_right_table_col.indexOf(key_col)
              join_right_key_col_type match {
                case "IntegerType" =>
                  _fpgaSWFuncCode += "        int32_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt32(i, " + join_right_key_col_idx + ");"
                case "LongType" =>
                  _fpgaSWFuncCode += "        int64_t " + join_right_key_col_name + " = " + tbl_in_2 + ".getInt64(i, " + join_right_key_col_idx + ");"
                case "StringType" =>
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1> " + join_right_key_col_name + "_n = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(key_col)) + " + 1>(i, " + join_right_key_col_idx + ");"
                  _fpgaSWFuncCode += "        std::string " + join_right_key_col_name + " = std::string(" + join_right_key_col_name + "_n.data());"
                case _ =>
                  _fpgaSWFuncCode += "        // Unsupported join key type"
              }
              key_str += join_right_key_col_name + ", "
            }
            _fpgaSWFuncCode += "        auto it = ht1.find(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "}" + ");"
            //  Payload
            for (right_payload <- rightTablePayloadColNames) {
              var right_payload_name = stripColumnName(right_payload)
              var right_payload_type = getColumnType(right_payload, dfmap)
              var right_payload_input_index = join_right_table_col.indexOf(right_payload)
              var right_payload_index = _outputCols.indexOf(right_payload)
              right_payload_type match {
                case "IntegerType" =>
                  _fpgaSWFuncCode += "        int32_t " + right_payload_name + " = " + tbl_in_2 + ".getInt32(i, " + right_payload_input_index + ");"
                case "LongType" =>
                  _fpgaSWFuncCode += "        int64_t " + right_payload_name + " = " + tbl_in_2 + ".getInt64(i, " + right_payload_input_index + ");"
                case "StringType" =>
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1> " + right_payload_name + " = " + tbl_in_2 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(i, " + right_payload_input_index + ");"
                case _ =>
                  _fpgaSWFuncCode += "        // Unsupported join key type"
              }
            }
            _fpgaSWFuncCode += "        if (it != ht1.end()) {"
            _fpgaSWFuncCode += "            auto its = ht1.equal_range(" + joinKeyTypeName + "{" + key_str.stripSuffix(", ") + "}" + ");"
            _fpgaSWFuncCode += "            auto it_it = its.first;"
            _fpgaSWFuncCode += "            while (it_it != its.second) {"
            for (left_payload <- leftTablePayloadColNames) {
              var left_payload_name = stripColumnName(left_payload)
              var left_payload_type = getColumnType(left_payload, dfmap)
              var left_payload_index = _outputCols.indexOf(left_payload)
              left_payload_type match {
                case "IntegerType" =>
                  _fpgaSWFuncCode += "                int32_t " + left_payload_name + " = (it_it->second)." + left_payload_name + ";"
                case "LongType" =>
                  _fpgaSWFuncCode += "                int64_t " + left_payload_name + " = (it_it->second)." + left_payload_name + ";"
                case "StringType" =>
                  _fpgaSWFuncCode += "                std::string " + left_payload_name + "_n = (it_it->second)." + left_payload_name + ";"
                  _fpgaSWFuncCode += "                std::array<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1> " + left_payload_name + "{};"
                  _fpgaSWFuncCode += "                memcpy(" + left_payload_name + ".data(), (" + left_payload_name + "_n).data(), (" + left_payload_name + "_n).length());"
                case _ =>
                  _fpgaSWFuncCode += "                // Unsupported join key type"
              }
            }
            if (join_filter_pairs.length > 0) { //filtering is need
              var filter_expr = getJoinFilterExpression(joining_expression(0))
              _fpgaSWFuncCode += "                if " + filter_expr + " {"
              for (left_payload <- leftTablePayloadColNames) {
                var left_payload_name = stripColumnName(left_payload)
                var left_payload_type = getColumnType(left_payload, dfmap)
                var left_payload_index = _outputCols.indexOf(left_payload)
                if (left_payload_index != -1) {
                  left_payload_type match {
                    case "IntegerType" =>
                      _fpgaSWFuncCode += "                    " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
                    case "LongType" =>
                      _fpgaSWFuncCode += "                    " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
                    case "StringType" =>
                      _fpgaSWFuncCode += "                    " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + ");"
                    case _ =>
                      _fpgaSWFuncCode += "                    // Unsupported join key type"
                  }
                }
              }
              for (right_payload <- rightTablePayloadColNames) {
                var right_payload_name = stripColumnName(right_payload)
                var right_payload_type = getColumnType(right_payload, dfmap)
                var right_payload_input_index = join_right_table_col.indexOf(right_payload)
                var right_payload_index = _outputCols.indexOf(right_payload)
                if (right_payload_index != -1) {
                  right_payload_type match {
                    case "IntegerType" =>
                      _fpgaSWFuncCode += "                    " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "LongType" =>
                      _fpgaSWFuncCode += "                    " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "StringType" =>
                      _fpgaSWFuncCode += "                    " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case _ =>
                      _fpgaSWFuncCode += "                    // Unsupported join key type"
                  }
                }
              }
              _fpgaSWFuncCode += "            }"
            }
            else { //no filtering - standard write out output columns
              for (left_payload <- leftTablePayloadColNames) {
                var left_payload_name = stripColumnName(left_payload)
                var left_payload_type = getColumnType(left_payload, dfmap)
                var left_payload_index = _outputCols.indexOf(left_payload)
                if (left_payload_index != -1) {
                  left_payload_type match {
                    case "IntegerType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + left_payload_name + ");"
                    case "LongType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + left_payload_name + ");"
                    case "StringType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + left_payload_name + ");"
                    case _ =>
                      _fpgaSWFuncCode += "                // Unsupported join key type"
                  }
                }
              }
              for (right_payload <- rightTablePayloadColNames) {
                var right_payload_name = stripColumnName(right_payload)
                var right_payload_type = getColumnType(right_payload, dfmap)
                var right_payload_input_index = join_right_table_col.indexOf(right_payload)
                var right_payload_index = _outputCols.indexOf(right_payload)
                if (right_payload_index != -1) {
                  right_payload_type match {
                    case "IntegerType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "LongType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "StringType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case _ =>
                      _fpgaSWFuncCode += "                // Unsupported join key type"
                  }
                }
              }
            }
            _fpgaSWFuncCode += "                it_it++;"
            _fpgaSWFuncCode += "                r++;"
            _fpgaSWFuncCode += "            }"
            _fpgaSWFuncCode += "        } else {"
            if (join_filter_pairs.length > 0) { //filtering is need
              var filter_expr = getJoinFilterExpression(joining_expression(0))
              _fpgaSWFuncCode += "            if " + filter_expr + " {"
              // Leave right table (i.e., left_payload) columns blank
              /* for (left_payload <- leftTablePayloadColNames) {
                  var left_payload_name = stripColumnName(left_payload)
                  var left_payload_type = getColumnType(left_payload, dfmap)
                  var left_payload_index = _outputCols.indexOf(left_payload)
                  if (left_payload_index != -1) {
                    left_payload_type match {
                      case "IntegerType" =>
                        _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + "0" + ");"
                      case "LongType" =>
                        _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + "0" + ");"
                      case "StringType" =>
                        _fpgaSWFuncCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + "\"\"" + ");"
                      case _ =>
                        _fpgaSWFuncCode += "                // Unsupported join key type"
                    }
                  }
                } */
              for (right_payload <- rightTablePayloadColNames) {
                var right_payload_name = stripColumnName(right_payload)
                var right_payload_type = getColumnType(right_payload, dfmap)
                var right_payload_input_index = join_right_table_col.indexOf(right_payload)
                var right_payload_index = _outputCols.indexOf(right_payload)
                if (right_payload_index != -1) {
                  right_payload_type match {
                    case "IntegerType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "LongType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "StringType" =>
                      _fpgaSWFuncCode += "                " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case _ =>
                      _fpgaSWFuncCode += "                // Unsupported join key type"
                  }
                }
              }
              _fpgaSWFuncCode += "            }"
            }
            else { //no filtering - standard write out output columns
              // Leave right table (i.e., left_payload) columns blank
              /* for (left_payload <- leftTablePayloadColNames) {
                  var left_payload_name = stripColumnName(left_payload)
                  var left_payload_type = getColumnType(left_payload, dfmap)
                  var left_payload_index = _outputCols.indexOf(left_payload)
                  if (left_payload_index != -1) {
                    left_payload_type match {
                      case "IntegerType" =>
                        _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt32(r, " + left_payload_index + ", " + "0" + ");"
                      case "LongType" =>
                        _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt64(r, " + left_payload_index + ", " + "0" + ");"
                      case "StringType" =>
                        _fpgaSWFuncCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(left_payload)) + " + 1>(r, " + left_payload_index + ", " + "\"\"" + ");"
                      case _ =>
                        _fpgaSWFuncCode += "            // Unsupported join key type"
                    }
                  }
                } */
              for (right_payload <- rightTablePayloadColNames) {
                var right_payload_name = stripColumnName(right_payload)
                var right_payload_type = getColumnType(right_payload, dfmap)
                var right_payload_input_index = join_right_table_col.indexOf(right_payload)
                var right_payload_index = _outputCols.indexOf(right_payload)
                if (right_payload_index != -1) {
                  right_payload_type match {
                    case "IntegerType" =>
                      _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt32(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "LongType" =>
                      _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt64(r, " + right_payload_index + ", " + right_payload_name + ");"
                    case "StringType" =>
                      _fpgaSWFuncCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(right_payload)) + " + 1>(r, " + right_payload_index + ", " + "\"\"" + ");"
                    case _ =>
                      _fpgaSWFuncCode += "            // Unsupported join key type"
                  }
                }
              }
            }
            _fpgaSWFuncCode += "            r++;"
            _fpgaSWFuncCode += "        }"
            _fpgaSWFuncCode += "    }"
            _fpgaSWFuncCode += "    " + tbl_out_1 + ".setNumRow(r);"
          case "Filter" =>
            // tag:filter
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children) {
              tempStr += "Table &" + ch.fpgaOutputTableName + ", "
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            tempStr += "Table &" + _fpgaOutputTableName + ") {"
            _fpgaSWFuncCode += tempStr
            var tbl_in_1 = _children.head.fpgaOutputTableName
            var tbl_out_1 = _fpgaOutputTableName

            println("------Filter Terms: " + _operation.head)
            println(filtering_expression.toString)
            //              if (_children.length > 1) //subquery involved
            //                println(getFilterExpression(filtering_expression, _children)._2)
            //              else
            //                println(getFilterExpression(filtering_expression)._2)
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Input: " + _children.head.outputCols
            _fpgaSWFuncCode += "    // Output: " + _outputCols
            _fpgaSWFuncCode += "    int r = 0;"
            _fpgaSWFuncCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
            _fpgaSWFuncCode += "    for (int i = 0; i < nrow1; i++) {"
            //print out filtering referenced columns
            for (_ref_col <- filtering_expression.references) {
              var filter_input_col_name = _ref_col.toString
              var filter_input_col_idx = _children.head.outputCols.indexOf(filter_input_col_name)
              var filter_input_col_type = getColumnType(filter_input_col_name, dfmap)
              filter_input_col_name = stripColumnName(filter_input_col_name)
              if (filter_input_col_type == "StringType") {
                _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(_ref_col.toString)) + " + 1> " + filter_input_col_name + " = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(_ref_col.toString)) + " + 1>(i, " + filter_input_col_idx.toString + ");"
              } else if (filter_input_col_type == "IntegerType") {
                _fpgaSWFuncCode += "        int32_t " + filter_input_col_name + " = " + tbl_in_1 + ".getInt32(i, " + filter_input_col_idx.toString + ");"
              } else if (filter_input_col_type == "LongType") {
                _fpgaSWFuncCode += "        int64_t " + filter_input_col_name + " = " + tbl_in_1 + ".getInt64(i, " + filter_input_col_idx.toString + ");"
              } else {
                _fpgaSWFuncCode += "        // Unsupported column type" + filter_input_col_type.toString + " default to IntegerType"
                _fpgaSWFuncCode += "        int32_t " + filter_input_col_name + " = " + tbl_in_1 + ".getInt32(i, " + filter_input_col_idx.toString + ");"
              }
            }
            //print out filtering expression
            if (_children.length > 1) {//subquery involved
              var tmp_var_str = getFilterExpression(filtering_expression, _children)
              for (reuse_col_str <- tmp_var_str._1) {
                _fpgaSWFuncCode += "        " + reuse_col_str
              }
              _fpgaSWFuncCode += "        if " + tmp_var_str._2 + " {"
            }
            else {
              var tmp_var_str = getFilterExpression(filtering_expression)
              for (reuse_col_str <- tmp_var_str._1) {
                _fpgaSWFuncCode += "        " + reuse_col_str
              }
              _fpgaSWFuncCode += "        if " + tmp_var_str._2 + " {"
            }
            var i = 0
            for (col <- _outputCols) {
              var dataType = getColumnType(col, dfmap)
              var col_symbol = stripColumnName(col)
              var col_idx = _children.head.outputCols.indexOf(col)
              if (col_idx == -1) { //not found - check output_alias
                col_idx = _children.head.outputCols.indexOf(_outputCols_alias(i))
              }
              dataType match {
                case "StringType" =>
                  if (_parent.head._stringRowIDSubstitution) {
                    // return row index
                    _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt32(r, " + i + ", i);"
                    _stringRowIDSubstitution = true
                    _fpgaInputTableName_stringRowIDSubstitute = tbl_in_1
                    _fpgaOutputTableName_stringRowIDSubstitute = _fpgaInputTableName_stringRowIDSubstitute
                  } else {
                    _fpgaSWFuncCode += "            std::array<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1> " + col_symbol + "_t = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1>(i, " + col_idx + ");"
                    _fpgaSWFuncCode += "            " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1>(r, " + i + ", " + col_symbol + "_t);"
                  }
                case "IntegerType" =>
                  _fpgaSWFuncCode += "            int32_t " + col_symbol + "_t = " + tbl_in_1 + ".getInt32(i, " + col_idx + ");"
                  _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt32(r, " + i + ", " + col_symbol + "_t);"
                case "LongType" =>
                  _fpgaSWFuncCode += "            int64_t " + col_symbol + "_t = " + tbl_in_1 + ".getInt64(i, " + col_idx + ");"
                  _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt64(r, " + i + ", " + col_symbol + "_t);"
                case _ =>
                  _fpgaSWFuncCode += "            // Unsupported column type" + dataType.toString + " default to IntegerType"
                  _fpgaSWFuncCode += "            int32_t " + col_symbol + "_t = " + tbl_in_1 + ".getInt32(i, " + col_idx + ");"
                  _fpgaSWFuncCode += "            " + tbl_out_1 + ".setInt32(r, " + i + ", " + col_symbol + "_t);"
              }
              i += 1
            }
            _fpgaSWFuncCode += "            r++;"
            _fpgaSWFuncCode += "        }"
            _fpgaSWFuncCode += "    }"
            _fpgaSWFuncCode += "    " + tbl_out_1 + ".setNumRow(r);"
          case "Aggregate" =>
            // tag:aggregate
            println("------Aggregate Terms: ")
            println(_operation)
            println("number of terms: " + _operation.length)
            println(_aggregate_operation)
            println("number of aggregate terms: " + _aggregate_operation.length)
            println(_groupBy_operation)
            println("number of groupBy terms: " + _groupBy_operation.length)

            var groupKeyName = ""
            var groupPayloadName = ""
            var groupByExists = false
            if (_groupBy_operation.length > 0) {
              groupByExists = true
            }
            if (groupByExists == true) {
              if (_groupBy_operation.length > 1) {
                // groupBy - creating the groupByKey struct
                groupKeyName = _fpgaSWFuncName + "_key"
                var equalOperatorStr = "    bool operator==(const " + groupKeyName + "& other) const { return"
                var first_groupBy_key = true
                _fpgaSWFuncCode += "struct " + groupKeyName + " {"
                for (groupBy_key <- _groupBy_operation) {
                  var key_type = getColumnType(groupBy_key, dfmap)
                  var key_col = stripColumnName(groupBy_key)
                  if (key_type == "IntegerType") {
                    _fpgaSWFuncCode += "    int32_t " + key_col + ";"
                  } else if (key_type == "LongType") {
                    _fpgaSWFuncCode += "    int64_t " + key_col + ";"
                  } else if (key_type == "StringType") {
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "    int32_t " + key_col + ";"
                    } else {
                      _fpgaSWFuncCode += "    std::string " + key_col + ";"
                    }
                  } else {
                    _fpgaSWFuncCode += "    // Unsupported Data Type"
                  }
                  if (first_groupBy_key == true) {
                    equalOperatorStr += " (" + key_col + " == other." + key_col + ")"
                    first_groupBy_key = false
                  } else {
                    equalOperatorStr += " && (" + key_col + " == other." + key_col + ")"
                  }
                }
                equalOperatorStr += "; }"
                _fpgaSWFuncCode += equalOperatorStr
                _fpgaSWFuncCode += "};"
                // groupBy - creating hash struct for groupByKey
                _fpgaSWFuncCode += "namespace std {"
                _fpgaSWFuncCode += "template <>"
                _fpgaSWFuncCode += "struct hash<" + groupKeyName + "> {"
                _fpgaSWFuncCode += "    std::size_t operator() (const " + groupKeyName + "& k) const {"
                _fpgaSWFuncCode += "        using std::size_t;"
                _fpgaSWFuncCode += "        using std::hash;"
                _fpgaSWFuncCode += "        using std::string;"
                var hashKeyStr = "        return "
                first_groupBy_key = true
                for (groupBy_key <- _groupBy_operation) {
                  var key_type = getColumnType(groupBy_key, dfmap)
                  var key_col = stripColumnName(groupBy_key)
                  if (first_groupBy_key == true) {
                    first_groupBy_key = false
                  } else {
                    hashKeyStr += " + "
                  }
                  if (key_type == "IntegerType") {
                    hashKeyStr += "(hash<int32_t>()(k." + key_col + "))"
                  }
                  else if (key_type == "LongType") {
                    hashKeyStr += "(hash<int64_t>()(k." + key_col + "))"
                  }
                  else if (key_type == "StringType") {
                    if (_stringRowIDSubstitution) {
                      hashKeyStr += "(hash<int32_t>()(k." + key_col + "))"
                    } else {
                      hashKeyStr += "(hash<string>()(k." + key_col + "))"
                    }
                  }
                  else {
                    _fpgaSWFuncCode += "    // Unsupported Data Type"
                  }
                }
                hashKeyStr += ";"
                _fpgaSWFuncCode += hashKeyStr
                _fpgaSWFuncCode += "    }"
                _fpgaSWFuncCode += "};"
                _fpgaSWFuncCode += "}"
              } else {
                groupKeyName = _fpgaSWFuncName + "_key"
                var key_type_name = "invalid data type"
                var key_type = getColumnType(_groupBy_operation(0), dfmap)
                var key_col = stripColumnName(_groupBy_operation(0))
                if (key_type == "IntegerType") {
                  key_type_name = "int32_t "
                } else if (key_type == "LongType") {
                  key_type_name = "int64_t "
                } else if (key_type == "StringType") {
                  if (_stringRowIDSubstitution) {
                    key_type_name = "int32_t "
                  } else {
                    key_type_name = "std::string "
                  }
                } else {
                  key_type_name = "invalid data type "
                }
                _fpgaSWFuncCode += "typedef " + key_type_name + groupKeyName + ";"
              }
              // groupBy - creating the groupByPayload struct
              groupPayloadName = _fpgaSWFuncName + "_payload"
              _fpgaSWFuncCode += "struct " + groupPayloadName + " {"
              var op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                var col_expr = getAggregateExpression(groupBy_payload.children(0))
                var col_type = groupBy_payload.dataType.toString
                println(col_symbol + " = " + col_expr + " : " + col_type)
                columnDictionary += (col_symbol -> (col_type, "NULL"))
                var key_type = getColumnType(col_symbol, dfmap)
                var col_symbol_trimmed = stripColumnName(col_symbol)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                if (col_aggregate_ops.isEmpty) { //Simple "alias" operation, without any aggregation operation, like: "l_partkey#78 AS l_partkey#78#396"
                  if (key_type == "IntegerType") {
                    _fpgaSWFuncCode += "    int32_t " + col_symbol_trimmed + ";"
                  } else if (key_type == "LongType") {
                    _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + ";"
                  } else if (key_type == "StringType") {
                    columnDictionary(col_symbol) = columnDictionary(groupBy_payload.references.head.toString)
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "    int32_t " + col_symbol_trimmed + ";"
                    }
                    else {
                      _fpgaSWFuncCode += "    std::string " + col_symbol_trimmed + ";"
                    }
                  } else if (key_type == "DoubleType") {
                    columnDictionary(col_symbol) = ("LongType", "NULL")
                    _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + ";"
                  } else {
                    _fpgaSWFuncCode += "    // Unsupported Data Type"
                  }
                }
                for (aggr_op <- col_aggregate_ops) {
                  var col_aggregate_op = aggr_op.toString.split("\\(").head
                  if (key_type == "IntegerType") {
                    _fpgaSWFuncCode += "    int32_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                  } else if (key_type == "LongType") {
                    _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                  } else if (key_type == "StringType") {
                    columnDictionary(col_symbol) = columnDictionary(groupBy_payload.references.head.toString)
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "    int32_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                    }
                    else {
                      _fpgaSWFuncCode += "    std::string " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                    }
                  } else if (key_type == "DoubleType") {
                    columnDictionary(col_symbol) = ("LongType", "NULL")
                    _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                  } else {
                    key_type = key_type.split("\\(").head
                    if (key_type == "DecimalType"){
                      columnDictionary(col_symbol) = ("LongType", "NULL")
                      _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "    // Unsupported output Type"
                    }
                  }
                  op_idx += 1
                }
              }
              _fpgaSWFuncCode += "};"
            }
            // aggregate - creating the SW function
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children) {
              if (sf == 30) {
                if (ch._cpuORfpga == 1) {
                  tempStr += "Table *" + ch.fpgaOutputTableName + ", "
                } else {
                  tempStr += "Table &" + ch.fpgaOutputTableName + ", "
                }
              } else {
                tempStr += "Table &" + ch.fpgaOutputTableName + ", "
              }
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            if (sf == 30 && _children.head._cpuORfpga == 1) {
              tempStr += "Table &" + _fpgaOutputTableName + ", int hpTimes) {"
            } else {
              tempStr += "Table &" + _fpgaOutputTableName + ") {"
            }
            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Input: " + _children.head.outputCols
            _fpgaSWFuncCode += "    // Output: " + outputCols
            if (groupByExists == true) {
              _fpgaSWFuncCode += "    std::unordered_map<" + groupKeyName + ", " + groupPayloadName + "> ht1;"
            }
            else {
              var op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_type = groupBy_payload.dataType.toString
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                for (aggr_op <- col_aggregate_ops) {
                  var col_aggregate_op = aggr_op.toString.split("\\(").head
                  if (col_type == "IntegerType") {
                    _fpgaSWFuncCode += "    int32_t " + col_aggregate_op + "_" + op_idx + " = 0;"
                  } else if (col_type == "LongType") {
                    _fpgaSWFuncCode += "    int64_t " + col_aggregate_op + "_" + op_idx + " = 0;"
                  } else if (col_type == "StringType") {
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "    int32_t " + col_aggregate_op + "_" + op_idx + " = 0;"
                    }
                    else {
                      _fpgaSWFuncCode += "    std::string " + col_aggregate_op + "_" + op_idx + " = \"\";"
                    }
                  } else if (col_type == "DoubleType") {
                    _fpgaSWFuncCode += "    int64_t " + col_aggregate_op + "_" + op_idx + " = 0;"
                  } else {
                    col_type = col_type.toString.split("\\(").head
                    if (col_type == "DecimalType"){
                      _fpgaSWFuncCode += "    int64_t " + col_aggregate_op + "_" + op_idx + " = 0;"
                    } else {
                      println(col_type + " : " + col_aggregate_op)
                      _fpgaSWFuncCode += "    // Unsupported Data Type"
                    }
                  }
                  op_idx += 1
                }
              }
            }
            var tbl_in_1 = _children.head.fpgaOutputTableName
            var tbl_out_1 = _fpgaOutputTableName
            var tbl_partition_suffix = ""
            if (sf == 30 && _children.head._cpuORfpga == 1) {
              _fpgaSWFuncCode += "for (int p_idx = 0; p_idx < hpTimes; p_idx++) {"
              tbl_partition_suffix = "[p_idx]"
            }
            _fpgaSWFuncCode += "    int nrow1 = " + tbl_in_1 + tbl_partition_suffix + ".getNumRow();"
            _fpgaSWFuncCode += "    for (int i = 0; i < nrow1; i++) {"
            var i = 0
            // print out all input columns
            for (ch <- _children.head.outputCols) {
              var input_col_type = getColumnType(ch, dfmap)
              var input_col = stripColumnName(ch)
              if (input_col_type == "IntegerType") {
                _fpgaSWFuncCode += "        int32_t " + input_col + " = " + tbl_in_1 + tbl_partition_suffix + ".getInt32(i, " + i + ");"
              } else if (input_col_type == "LongType") {
                _fpgaSWFuncCode += "        int64_t " + input_col + " = " + tbl_in_1 + tbl_partition_suffix + ".getInt64(i, " + i + ");"
              } else if (input_col_type == "StringType") {
                if (_stringRowIDBackSubstitution) {
                  //find the original stringRowID table that contains this string data
                  var orig_table_names = get_stringRowIDOriginalTableName(this)
                  var orig_table_columns = get_stringRowIDOriginalTableColumns(this)
                  var orig_tbl_idx = -1
                  var orig_tbl_col_idx = -1
                  for (orig_tbl <- orig_table_columns) {
                    for (col <- orig_tbl) {
                      if (columnDictionary(ch) == columnDictionary(col)) {
                        orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                        orig_tbl_col_idx = orig_tbl.indexOf(col)
                      }
                    }
                  }
                  //find the col index of the string data in the original stringRowID table
                  if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
                    _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1> " + input_col + " = " + tbl_in_1 + tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1>(i, " + i + ");"
                  }
                  else {
                    var rowIDNum = tbl_in_1 + tbl_partition_suffix + ".getInt32(i, " + i + ")"
                    _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1> " + input_col + " = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
                  }
                }
                else if (_stringRowIDSubstitution) {
                  //TODO: complete this, use int32_t rowid datatyep
                  _fpgaSWFuncCode += "        int32_t " + input_col + " = " + tbl_in_1 + tbl_partition_suffix + ".getInt32(i, " + i + ");"
                }
                else {
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1> " + input_col + " = " + tbl_in_1 + tbl_partition_suffix + ".getcharN<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1>(i, " + i + ");"
                }
              } else {
                _fpgaSWFuncCode += "        // Unsupported input Type"
              }
              i += 1
            }

            if (groupByExists == true) {
              // print groupBy key variable
              var groupByKeyStr = "        " + groupKeyName + " k{"
              var first_groupBy_key = true
              for (groupBy_key <- _groupBy_operation) {
                var key_type = getColumnType(groupBy_key, dfmap)
                var key_col = stripColumnName(groupBy_key)
                if (first_groupBy_key == true) {
                  first_groupBy_key = false
                } else {
                  groupByKeyStr += ", "
                }
                if (key_type == "IntegerType") {
                  groupByKeyStr += key_col
                } else if (key_type == "LongType") {
                  groupByKeyStr += key_col
                } else if (key_type == "StringType") {
                  if (_stringRowIDSubstitution) {
                    groupByKeyStr += key_col
                  }
                  else {
                    groupByKeyStr += "std::string(" + key_col + ".data())"
                  }
                } else {
                  groupByKeyStr += "    // Unsupported Data Type"
                }
              }
              groupByKeyStr += "};"
              if (_groupBy_operation.length == 1) {
                groupByKeyStr = "        " + groupKeyName + " k = "
                var key_type = getColumnType(_groupBy_operation(0), dfmap)
                var key_col = stripColumnName(_groupBy_operation(0))
                if (key_type == "StringType") {
                  if (_stringRowIDSubstitution) {
                    groupByKeyStr += key_col + ";"
                  }
                  else {
                    groupByKeyStr += "std::string(" + key_col + ".data());"
                  }
                } else {
                  groupByKeyStr += key_col + ";"
                }
              }
              _fpgaSWFuncCode += groupByKeyStr
              // print aggregate columns with expression
              var op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_type = groupBy_payload.dataType.toString
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                columnDictionary += (col_symbol -> (col_type, "NULL"))
                var col_symbol_trimmed = stripColumnName(col_symbol)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                if (col_aggregate_ops.isEmpty) {
                  var col_expr = getAggregateExpression(groupBy_payload.children(0))
                  if (col_type == "IntegerType") {
                    _fpgaSWFuncCode += "        int32_t " + col_symbol_trimmed + " = " + col_expr + ";"
                  } else if (col_type == "LongType") {
                    _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + " = " + col_expr + ";"
                  } else if (col_type == "StringType") {
                    columnDictionary(col_symbol_trimmed) = columnDictionary(groupBy_payload.references.head.toString)
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "        int32_t " + col_symbol_trimmed + " = " + col_expr + ";"
                    }
                    else {
                      _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col_symbol_trimmed)) + " + 1> " + col_symbol_trimmed + " = " + col_expr + ";"
                    }
                  } else if (col_type == "DoubleType") {
                    columnDictionary(col_symbol) = ("LongType", "NULL")
                    _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + " = " + col_expr + ";"
                  } else {
                    _fpgaSWFuncCode += "        // Unsupported output Type"
                  }
                }
                for (aggr_op <- col_aggregate_ops) {
                  var col_aggregate_op = aggr_op.toString.split("\\(").head
                  var col_expr = getAggregateExpression(aggr_op)
                  // temp hack by Alec - for count operation such as "count(o_orderkey#207)"
                  if (col_aggregate_op == "count") {
                    col_expr += " != 0 ? 1 : 0"
                  }
                  if (col_type == "IntegerType") {
                    _fpgaSWFuncCode += "        int32_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                  } else if (col_type == "LongType") {
                    _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                  } else if (col_type == "StringType") {
                    columnDictionary(col_symbol_trimmed) = columnDictionary(groupBy_payload.references.head.toString)
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "        int32_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                    } else {
                      _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col_symbol_trimmed)) + " + 1> " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                    }
                  } else if (col_type == "DoubleType") {
                    columnDictionary(col_symbol) = ("LongType", "NULL")
                    _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                  } else {
                    col_type = col_type.split("\\(").head
                    if (col_type == "DecimalType"){
                      columnDictionary(col_symbol) = ("LongType", "NULL")
                      _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported output Type"
                    }
                  }
                  op_idx += 1
                }
              }
              // print groupBy payload variable
              var groupByPayloadStr = "        " + groupPayloadName + " p{"
              var first_groupBy_payload = true
              op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                var col_type = groupBy_payload.dataType.toString
                col_symbol = stripColumnName(col_symbol)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                if (col_aggregate_ops.isEmpty) {
                  if (first_groupBy_payload == true) {
                    first_groupBy_payload = false
                  } else {
                    groupByPayloadStr += ", "
                  }
                  if (col_type == "IntegerType") {
                    groupByPayloadStr += col_symbol
                  } else if (col_type == "LongType") {
                    groupByPayloadStr += col_symbol
                  } else if (col_type == "StringType") {
                    if (_stringRowIDSubstitution) {
                      groupByPayloadStr += col_symbol
                    } else {
                      groupByPayloadStr += "std::string(" + col_symbol + ".data())"
                    }
                  } else if (col_type == "DoubleType") {
                    groupByPayloadStr += col_symbol
                  } else {
                    groupByPayloadStr += "    // Unsupported Data Type"
                  }
                }
                for (aggr_op <- col_aggregate_ops) {
                  if (first_groupBy_payload == true) {
                    first_groupBy_payload = false
                  } else {
                    groupByPayloadStr += ", "
                  }
                  var col_aggregate_op = aggr_op.toString.split("\\(").head
                  if (col_type == "IntegerType") {
                    groupByPayloadStr += col_symbol + "_" + col_aggregate_op + "_" + op_idx
                  } else if (col_type == "LongType") {
                    groupByPayloadStr += col_symbol + "_" + col_aggregate_op + "_" + op_idx
                  } else if (col_type == "StringType") {
                    if (_stringRowIDSubstitution) {
                      groupByPayloadStr += col_symbol + "_" + col_aggregate_op + "_" + op_idx
                    } else {
                      groupByPayloadStr += "std::string(" + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ".data()"
                    }
                  } else if (col_type == "DoubleType") {
                    groupByPayloadStr += col_symbol + "_" + col_aggregate_op + "_" + op_idx
                  } else {
                    col_type = col_type.split("\\(").head
                    if (col_type == "DecimalType"){
                      groupByPayloadStr += col_symbol + "_" + col_aggregate_op + "_" + op_idx
                    } else {
                      groupByPayloadStr += "    // Unsupported output Type"
                    }
                  }
                  op_idx += 1
                }
              }
              groupByPayloadStr += "};"
              _fpgaSWFuncCode += groupByPayloadStr
              // assign groupBy payload
              _fpgaSWFuncCode += "        auto it = ht1.find(k);"
              _fpgaSWFuncCode += "        if (it != ht1.end()) {"
              op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                var col_op = groupBy_payload.toString.split("\\(").head
                var col_type = getColumnType(col_symbol, dfmap)
                col_symbol = stripColumnName(col_symbol)
                println(col_op + " : " + col_type)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                if (col_aggregate_ops.isEmpty) {

                }
                for (aggr_op <- col_aggregate_ops) {
                  var col_aggregate_op = aggr_op.toString.split("\\(").head
                  if (col_aggregate_op == "sum") {
                    if (col_type == "IntegerType") {
                      _fpgaSWFuncCode += "            int32_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else if (col_type == "LongType") {
                      _fpgaSWFuncCode += "            int64_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "            // Unsupported payload Type"
                    }
                    _fpgaSWFuncCode += "            p." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " = " + col_aggregate_op + "_" + op_idx + ";"
                  }
                  else if (col_aggregate_op == "count") {
                    if (col_type == "IntegerType") {
                      _fpgaSWFuncCode += "            int32_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else if (col_type == "LongType") {
                      _fpgaSWFuncCode += "            int64_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "            // Unsupported payload Type"
                    }
                    _fpgaSWFuncCode += "            p." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " = " + col_aggregate_op + "_" + op_idx + ";"
                  }
                  else if (col_aggregate_op == "min") {
                    if (col_type == "IntegerType") {
                      _fpgaSWFuncCode += "            int32_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " > " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " ? " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " : " + "(it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else if (col_type == "LongType") {
                      _fpgaSWFuncCode += "            int64_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " > " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " ? " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " : " + "(it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "            // Unsupported payload Type"
                    }
                    _fpgaSWFuncCode += "            p." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " = " + col_aggregate_op + "_" + op_idx + ";"
                  }
                  else if (col_aggregate_op == "max") {
                    if (col_type == "IntegerType") {
                      _fpgaSWFuncCode += "            int32_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " < " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " ? " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " : " + "(it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else if (col_type == "LongType") {
                      _fpgaSWFuncCode += "            int64_t " + col_aggregate_op + "_" + op_idx + " = (it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " < " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " ? " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " : " + "(it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "            // Unsupported payload Type"
                    }
                    _fpgaSWFuncCode += "            p." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " = " + col_aggregate_op + "_" + op_idx + ";"
                  }
                  else if (col_aggregate_op == "avg") {
                    if (col_type == "IntegerType") {
                      // FIXME: optimization to reduce number of division op in avg()
                      // _fpgaSWFuncCode += "            int32_t " + col_aggregate_op + "_" + op_idx + " = ((it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " * i + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ") / (i+1);"
                      _fpgaSWFuncCode += "            int32_t " + col_aggregate_op + "_" + op_idx + " = ((it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ");"
                    } else if (col_type == "LongType") {
                      //_fpgaSWFuncCode += "            int64_t " + col_aggregate_op + "_" + op_idx + " = ((it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " * i + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ") / (i+1);"
                      _fpgaSWFuncCode += "            int64_t " + col_aggregate_op + "_" + op_idx + " = ((it->second)." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " + " + col_symbol + "_" + col_aggregate_op + "_" + op_idx + ");"
                    } else {
                      _fpgaSWFuncCode += "            // Unsupported payload Type"
                    }
                    _fpgaSWFuncCode += "            p." + col_symbol + "_" + col_aggregate_op + "_" + op_idx + " = " + col_aggregate_op + "_" + op_idx + ";"
                  }
                  else {
                    if (!groupBy_payload.toString.contains("(") && !groupBy_payload.toString.contains(")")){
                      //Simple re-assignment using " AS " - nothing to do...
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported aggregation operation"
                    }
                  }
                  op_idx += 1
                }
              }
              _fpgaSWFuncCode += "            ht1[k] = p;"
              _fpgaSWFuncCode += "        } else { "
              _fpgaSWFuncCode += "            ht1.insert(std::make_pair(k, p));"
              _fpgaSWFuncCode += "        }"
            }
            else {
              // print aggregate columns with expression
              var op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_type = groupBy_payload.dataType.toString
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                columnDictionary += (col_symbol -> (col_type, "NULL"))
                var col_symbol_trimmed = stripColumnName(col_symbol)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                for (aggr_op <- col_aggregate_ops) {
                  var col_aggregate_op = aggr_op.toString.split("\\(").head
                  var col_expr = getAggregateExpression(aggr_op)
                  if (col_type == "IntegerType") {
                    _fpgaSWFuncCode += "        int32_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                  } else if (col_type == "LongType") {
                    _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                  } else if (col_type == "StringType") {
                    columnDictionary(col_symbol_trimmed) = columnDictionary(groupBy_payload.references.head.toString)
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "        int32_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                    } else {
                      _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col_symbol_trimmed)) + " + 1> " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                    }
                  } else if (col_type == "DoubleType") {
                    columnDictionary(col_symbol) = ("LongType", "NULL")
                    _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                  } else {
                    col_type = col_type.split("\\(").head
                    if (col_type == "DecimalType"){
                      columnDictionary(col_symbol) = ("LongType", "NULL")
                      _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " = " + col_expr + ";"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported output Type"
                    }
                  }
                  op_idx += 1
                }
              }
              // assign groupBy payload
              op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                var col_type = getColumnType(col_symbol, dfmap)
                var col_symbol_trimmed = stripColumnName(col_symbol)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                for (aggr_op <- col_aggregate_ops) {
                  var col_aggregate_op = aggr_op.toString.split("\\(").head
                  if (col_aggregate_op == "sum") {
                    if (col_type == "IntegerType" || col_type == "LongType") {
                      _fpgaSWFuncCode += "        sum" + "_" + op_idx + " += " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload Type"
                    }
                  }
                  else if (col_aggregate_op == "count") {
                    if (col_type == "IntegerType" || col_type == "LongType") {
                      _fpgaSWFuncCode += "        count" + "_" + op_idx + " += " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload Type"
                    }
                  }
                  else if (col_aggregate_op == "min") {
                    if (col_type == "IntegerType" || col_type == "LongType") {
                      _fpgaSWFuncCode += "        min" + "_" + op_idx + " = min" + "_" + op_idx + " > " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " ? " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " : min" + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload Type"
                    }
                  }
                  else if (col_aggregate_op == "max") {
                    if (col_type == "IntegerType" || col_type == "LongType") {
                      _fpgaSWFuncCode += "        max" + "_" + op_idx + " = max" + "_" + op_idx + " < " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " ? " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + " : max" + "_" + op_idx + ";"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload Type"
                    }
                  }
                  else if (col_aggregate_op == "avg") {
                    if (col_type == "IntegerType" || col_type == "LongType") {
                      // _fpgaSWFuncCode += "        avg" + "_" + op_idx + " = (avg" + "_" + op_idx + " * i + " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ") / (i+1);"
                      _fpgaSWFuncCode += "        avg" + "_" + op_idx + " = (avg" + "_" + op_idx + " + " + col_symbol_trimmed + "_" + col_aggregate_op + "_" + op_idx + ");"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload Type"
                    }
                  }
                  else {
                    _fpgaSWFuncCode += "        // Unsupported aggregation operation"
                  }
                  op_idx += 1
                }
              }
            }
            _fpgaSWFuncCode += "    }"
            if (sf == 30 && _children.head._cpuORfpga == 1) {
              _fpgaSWFuncCode += "}"
            }

            // generate table output
            _fpgaSWFuncCode += "    int r = 0;"
            if (groupByExists == true) {
              // always assume the output table column order is {groupBy_key} then {groupBy_payload}
              _fpgaSWFuncCode += "    for (auto& it : ht1) {"
              if (_groupBy_operation.length > 1) {
                for (groupBy_key <- _groupBy_operation) {
                  var key_type = getColumnType(groupBy_key, dfmap)
                  var key_col = stripColumnName(groupBy_key)
                  var outputCols_idx = outputCols.indexOf(groupBy_key)
                  if (outputCols_idx < 0) { //not found in the output table
                    _fpgaSWFuncCode += "        // " + key_col + " not required in the output table"
                  } else { // columns to output
                    if (key_type == "IntegerType") {
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + outputCols_idx + ", (it.first)." + key_col + ");"
                    } else if (key_type == "LongType") {
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(r, " + outputCols_idx + ", (it.first)." + key_col + ");"
                    } else if (key_type == "StringType") {
                      if (_stringRowIDSubstitution) {
                        _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + outputCols_idx + ", (it.first)." + key_col + ");"
                      } else {
                        _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(groupBy_key)) + " + 1> " + key_col + "{};"
                        _fpgaSWFuncCode += "        memcpy(" + key_col + ".data(), ((it.first)." + key_col + ").data(), ((it.first)." + key_col + ").length());"
                        _fpgaSWFuncCode += "        " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(groupBy_key)) + " + 1>(r, " + outputCols_idx + ", " + key_col + ");"
                      }
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload type"
                    }
                  }
                }
              } else {
                var key_type = getColumnType(_groupBy_operation(0), dfmap)
                var key_col = stripColumnName(_groupBy_operation(0))
                var outputCols_idx = outputCols.indexOf(_groupBy_operation(0))
                if (outputCols_idx < 0) { //not found in the output table
                  _fpgaSWFuncCode += "        // " + key_col + " not required in the output table"
                } else {
                  if (key_type == "IntegerType") {
                    _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + outputCols_idx + ", (it.first));"
                  } else if (key_type == "LongType") {
                    _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(r, " + outputCols_idx + ", (it.first));"
                  } else if (key_type == "StringType") {
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + outputCols_idx + ", (it.first));"
                    } else {
                      _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(_groupBy_operation(0))) + " + 1> " + key_col + "{};"
                      _fpgaSWFuncCode += "        memcpy(" + key_col + ".data(), (it.first).data(), (it.first).length());"
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(_groupBy_operation(0))) + " + 1>(r, " + outputCols_idx + ", " + key_col + ");"
                    }
                  } else {
                    _fpgaSWFuncCode += "        // Unsupported payload type"
                  }
                }
              }
              var op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                var col_type = getColumnType(col_symbol, dfmap)
                var outputCols_idx = outputCols.indexOf(col_symbol)
                col_symbol = stripColumnName(col_symbol)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                if (!col_aggregate_ops.isEmpty){
                  var col_expr = getAggregateExpression_abstracted(groupBy_payload.children(0), col_aggregate_ops, op_idx, "(it.second)."+col_symbol+"_")
                  if (col_type == "IntegerType") {
                    _fpgaSWFuncCode += "        int32_t " + col_symbol + " = " + col_expr + ";"
                  } else if (col_type == "LongType") {
                    _fpgaSWFuncCode += "        int64_t " + col_symbol + " = " + col_expr + ";"
                  } else if (col_type == "StringType") {
                    if (_stringRowIDSubstitution) {
                      _fpgaSWFuncCode += "        int32_t " + col_symbol + " = " + col_expr + ";"
                    } else {
                      _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col_symbol)) + " + 1> " + col_symbol + " = " + col_expr + ";"
                    }
                  } else if (col_type == "DoubleType") {
                    _fpgaSWFuncCode += "        int64_t " + col_symbol + " = " + col_expr + ";"
                  } else {
                    col_type = col_type.split("\\(").head
                    if (col_type == "DecimalType"){
                      _fpgaSWFuncCode += "        int64_t " + col_symbol + " = " + col_expr + ";"
                      col_type = "LongType"
                    } else {
                      _fpgaSWFuncCode += "    // Unsupported output Type"
                    }
                  }
                  if (outputCols_idx < 0) { //not found in the output table
                    _fpgaSWFuncCode += "        // " + col_symbol + " not required in the output table"
                  } else { // columns to output
                    if (col_type == "IntegerType") {
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + outputCols_idx + ", " + col_symbol + ");"
                    } else if (col_type == "LongType") {
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(r, " + outputCols_idx + ", " + col_symbol + ");"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload type: " + col_type
                    }
                  }
                  op_idx += col_aggregate_ops.length
                }
                else {
                  if (outputCols_idx < 0) { //not found in the output table
                    _fpgaSWFuncCode += "        // " + col_symbol + " not required in the output table"
                  }
                  else { // columns to output
                    if (col_type == "IntegerType") {
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + outputCols_idx + ", (it.second)." + col_symbol + ");"
                    } else if (col_type == "LongType") {
                      _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(r, " + outputCols_idx + ", (it.second)." + col_symbol + ");"
                    } else {
                      _fpgaSWFuncCode += "        // Unsupported payload type: " + col_type
                    }
                  }
                }
              }
              _fpgaSWFuncCode += "        ++r;"
              _fpgaSWFuncCode += "    }"
            }
            else {
              // always assume the output table columns are {groupBy_payload}
              var op_idx = 0
              for (groupBy_payload <- _aggregate_expression) {
                var col_symbol = groupBy_payload.toString.split(" AS ").last
                var col_type = getColumnType(col_symbol, dfmap)
                var col_symbol_trimmed = stripColumnName(col_symbol)
                var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
                var col_expr = getAggregateExpression_abstracted(groupBy_payload.children(0), col_aggregate_ops, op_idx, "")
                if (col_type == "IntegerType") {
                  _fpgaSWFuncCode += "    int32_t " + col_symbol_trimmed + " = " + col_expr + ";"
                } else if (col_type == "LongType") {
                  _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + " = " + col_expr + ";"
                } else if (col_type == "StringType") {
                  columnDictionary(col_symbol_trimmed) = columnDictionary(groupBy_payload.references.head.toString)
                  if (_stringRowIDSubstitution) {
                    _fpgaSWFuncCode += "    int32_t " + col_symbol_trimmed + " = " + col_expr + ";"
                  } else {
                    _fpgaSWFuncCode += "    std::array<char, " + getStringLengthMacro(columnDictionary(col_symbol_trimmed)) + " + 1> " + col_symbol_trimmed + " = " + col_expr + ";"
                  }
                } else if (col_type == "DoubleType") {
                  columnDictionary(col_symbol) = ("LongType", "NULL")
                  _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + " = " + col_expr + ";"
                } else {
                  col_type = col_type.split("\\(").head
                  if (col_type == "DecimalType"){
                    columnDictionary(col_symbol) = ("LongType", "NULL")
                    _fpgaSWFuncCode += "    int64_t " + col_symbol_trimmed + " = " + col_expr + ";"
                    col_type = "LongType"
                  } else {
                    _fpgaSWFuncCode += "    // Unsupported output Type"
                  }
                }
                var outputCols_idx = outputCols.indexOf(col_symbol)
                if (outputCols_idx < 0)
                  _fpgaSWFuncCode += "    // Error: Cannot find the col_symbol in the output table."
                if (col_type == "IntegerType") {
                  _fpgaSWFuncCode += "    " + tbl_out_1 + ".setInt32(r++, " + outputCols_idx + ", " + col_symbol_trimmed + ");"
                  //Alec-added: adding aggregation result based on the "AS" name in the case of subquery
                  columnDictionary += (col_symbol_trimmed -> (tbl_out_1 + ".getInt32(" + op_idx + ", 0)", "NULL"))
                } else if (col_type == "LongType") {
                  _fpgaSWFuncCode += "    " + tbl_out_1 + ".setInt64(r++, " + outputCols_idx + ", " + col_symbol_trimmed + ");"
                  //Alec-added: adding aggregation result based on the "AS" name in the case of subquery
                  columnDictionary += (col_symbol_trimmed -> (tbl_out_1 + ".getInt64(" + op_idx + ", 0)", "NULL"))
                } else {
                  _fpgaSWFuncCode += "    // Unsupported payload type"
                }
                op_idx += col_aggregate_ops.length
              }
            }
            _fpgaSWFuncCode += "    " + tbl_out_1 + ".setNumRow(r);"
          case "Project" =>
            // tag:project
            println("------Project Terms: ")
            println(_operation)
            println("number of terms: " + _operation.length)
            println(_aggregate_operation)
            println("number of terms: " + _aggregate_operation.length)
            println(_aggregate_expression)
            println("aggre. expression: " + _aggregate_expression)
            // Creating the SW function
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children) {
              tempStr += "Table &" + ch.fpgaOutputTableName + ", "
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            tempStr += "Table &" + _fpgaOutputTableName + ") {"
            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Input: " + _children.head.outputCols
            _fpgaSWFuncCode += "    // Output: " + outputCols

            var tbl_in_1 = _children.head.fpgaOutputTableName
            var tbl_out_1 = _fpgaOutputTableName
            _fpgaSWFuncCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
            _fpgaSWFuncCode += "    for (int i = 0; i < nrow1; i++) {"
            var i = 0
            for (ch <- _children.head.outputCols) {
              var input_col_type = getColumnType(ch, dfmap)
              var input_col_symbol = stripColumnName(ch)
              if (input_col_type == "IntegerType") {
                _fpgaSWFuncCode += "        int32_t " + input_col_symbol + " = " + tbl_in_1 + ".getInt32(i, " + i + ");"
              } else if (input_col_type == "LongType") {
                _fpgaSWFuncCode += "        int64_t " + input_col_symbol + " = " + tbl_in_1 + ".getInt64(i, " + i + ");"
              } else if (input_col_type == "StringType") {

                if (_stringRowIDBackSubstitution) {
                  //find the original stringRowID table that contains this string data
                  var orig_table_names = get_stringRowIDOriginalTableName(this)
                  var orig_table_columns = get_stringRowIDOriginalTableColumns(this)
                  var orig_tbl_idx = -1
                  var orig_tbl_col_idx = -1
                  for (orig_tbl <- orig_table_columns) {
                    for (orig_col <- orig_tbl) {
                      if (columnDictionary(ch) == columnDictionary(orig_col)) {
                        orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                        orig_tbl_col_idx = orig_tbl.indexOf(orig_col)
                      }
                    }
                  }
                  //find the col index of the string data in the original stringRowID table
                  if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
                    _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1> " + input_col_symbol + " = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1>(i, " + i + ");"
                  }
                  else {
                    var rowIDNum = tbl_in_1 + ".getInt32(i, " + i + ")"
                    _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1> " + input_col_symbol + " = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
                  }
                }
                else {
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1> " + input_col_symbol + " = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(ch)) + " + 1>(i, " + i + ");"
                }
              } else {
                _fpgaSWFuncCode += "        // Unsupported input Type"
              }
              i += 1
            }
            var seen = new Array[Int](outputCols.length)
            for (expr <- _aggregate_expression) {
              var col_symbol = expr.toString.split(" AS ").last
              var col_expr = getAggregateExpression(expr.children(0))
              var col_type = expr.dataType.toString
              println(col_symbol + " = " + col_expr + " : " + col_type)
              columnDictionary += (col_symbol -> (col_type, "NULL"))
              var outputCols_idx = outputCols.indexOf(col_symbol)
              seen(outputCols_idx) = 1
              var col_symbol_trimmed = stripColumnName(col_symbol)
              //col_expr = stripColumnName(col_expr)
              if (col_type == "IntegerType") {
                _fpgaSWFuncCode += "        int32_t " + col_symbol_trimmed + " = " + col_expr + ";"
                _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(i, " + outputCols_idx + ", " + col_symbol_trimmed + ");"
              } else if (col_type == "LongType") {
                _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + " = " + col_expr + ";"
                _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(i, " + outputCols_idx + ", " + col_symbol_trimmed + ");"
              } else if (col_type == "StringType") {
                if (expr.children(0).getClass.getName == "org.apache.spark.sql.catalyst.expressions.Substring") {
                  columnDictionary(col_symbol) = (col_type, expr.children(0).children(2).toString);
                  _fpgaSWFuncCode += "        std::string " + col_symbol_trimmed + "_str" + " = " + col_expr + ";"
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col_symbol)) + " + 1> " + col_symbol_trimmed + "{};"
                  _fpgaSWFuncCode += "        memcpy(" + col_symbol_trimmed + ".data(), " + col_symbol_trimmed + "_str.data(), " + "(" + col_symbol_trimmed + "_str).length());"
                  _fpgaSWFuncCode += "        " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(col_symbol)) + " + 1>(i, " + outputCols_idx + ", " + col_symbol_trimmed + ");"
                } else {
                  columnDictionary(col_symbol) = columnDictionary(expr.references.head.toString)
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col_symbol)) + " + 1> " + col_symbol_trimmed + " = " + col_expr + ";"
                  _fpgaSWFuncCode += "        " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(col_symbol)) + " + 1>(i, " + outputCols_idx + ", " + col_symbol_trimmed + ");"
                }
              } else if (col_type == "DoubleType") {
                columnDictionary(col_symbol) = ("LongType", "NULL")
                _fpgaSWFuncCode += "        int64_t " + col_symbol_trimmed + " = " + col_expr + ";"
                _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(i, " + outputCols_idx + ", " + col_symbol_trimmed + ");"
              } else {
                _fpgaSWFuncCode += "        // Unsupported output Type"
              }
            }
            for (col <- outputCols) {
              var outputCols_idx = outputCols.indexOf(col)
              if (seen(outputCols_idx) == 0) {
                var input_col_type = getColumnType(col, dfmap)
                var input_col_symbol = stripColumnName(col)
                if (input_col_type == "IntegerType") {
                  _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(i, " + outputCols_idx + ", " + input_col_symbol + ");"
                } else if (input_col_type == "LongType") {
                  _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(i, " + outputCols_idx + ", " + input_col_symbol + ");"
                } else if (input_col_type == "StringType") {
                  _fpgaSWFuncCode += "        " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(input_col_symbol)) + " + 1>(i, " + outputCols_idx + ", " + input_col_symbol + ");"
                } else if (input_col_type == "DoubleType") {
                  _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(i, " + outputCols_idx + ", " + input_col_symbol + ");"
                } else {
                  _fpgaSWFuncCode += "        // Unsupported input Type"
                }
              }
            }
            _fpgaSWFuncCode += "    }"
            _fpgaSWFuncCode += "    " + tbl_out_1 + ".setNumRow(nrow1);"
          case "Sort" =>
            // tag:sort
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children){
              tempStr += "Table &" + ch.fpgaOutputTableName + ", "
            }
            if (_stringRowIDBackSubstitution == true) {
              var orig_table_names = get_stringRowIDOriginalTableName(this)
              for (orig_tbl <- orig_table_names){
                tempStr += "Table &" + orig_tbl + ", "
              }
            }
            tempStr += "Table &" + _fpgaOutputTableName + ") {"
            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // StringRowIDSubstitution: " + _stringRowIDSubstitution + " StringRowIDBackSubstitution: " + _stringRowIDBackSubstitution
            _fpgaSWFuncCode += "    // Supported operation: " + _nodeType
            _fpgaSWFuncCode += "    // Operation: " + _operation
            _fpgaSWFuncCode += "    // Input: " + _children.head.outputCols
            _fpgaSWFuncCode += "    // Output: " + outputCols

            var tbl_in_1 = _children.head.fpgaOutputTableName
            var tbl_out_1 = _fpgaOutputTableName

            // Creating the struct for row data
            var sortRowName = _fpgaSWFuncName + "Row"
            _fpgaSWFuncCode += "    struct " + sortRowName + " {"
            for (col <- outputCols) {
              var output_col_type = getColumnType(col, dfmap)
              var output_col_symbol = stripColumnName(col)
              if (output_col_type == "IntegerType") {
                _fpgaSWFuncCode += "        int32_t " + output_col_symbol + ";"
              }
              else if (output_col_type == "LongType") {
                _fpgaSWFuncCode += "        int64_t " + output_col_symbol + ";"
              }
              else if (output_col_type == "DoubleType") {
                _fpgaSWFuncCode += "        int32_t " + output_col_symbol + ";"
              }
              else if (output_col_type == "StringType") {
                _fpgaSWFuncCode += "        std::string " + output_col_symbol + ";"
              }
              else {
                _fpgaSWFuncCode += "        // Unsupported input column type"
              }
            }
            _fpgaSWFuncCode += "    }; \n"
            // Creating the lamda function for the sort algo
            var sortLamdaFuncName = _fpgaSWFuncName + "_order"
            var sortLamdaFuncStr = "        bool operator()(const " + sortRowName + "& a, const "+ sortRowName + "& b) const { return \n"
            var sortLamdaFuncConditionStr = ""
            var firstSortLamdaFuncCondition = true
            _fpgaSWFuncCode += "    struct {"
            for (sorting_expr <- sorting_expression) {
              var col_symbol = sorting_expr.children(0)
              var col_direction = sorting_expr.toString.split(" ")(1)
              var sort_col = stripColumnName(col_symbol.toString)
              println(sort_col + " : " + col_direction)
              if (firstSortLamdaFuncCondition == true){
                firstSortLamdaFuncCondition = false
                if (col_direction == "DESC") {
                  sortLamdaFuncStr += " (a." + sort_col + " > b." + sort_col + ")"
                } else if (col_direction == "ASC") {
                  sortLamdaFuncStr += " (a." + sort_col + " < b." + sort_col + ")"
                }
              } else {
                sortLamdaFuncStr += " || \n ("
                sortLamdaFuncStr += sortLamdaFuncConditionStr
                if (col_direction == "DESC") {
                  sortLamdaFuncStr += "(a." + sort_col + " > b." + sort_col + "))"
                } else if (col_direction == "ASC") {
                  sortLamdaFuncStr += "(a." + sort_col + " < b." + sort_col + "))"
                }
              }
              sortLamdaFuncConditionStr += "(a." + sort_col + " == b." + sort_col + ") && "
            }
            sortLamdaFuncStr += "; \n}"
            _fpgaSWFuncCode += sortLamdaFuncStr
            _fpgaSWFuncCode += "    }" + sortLamdaFuncName + "; \n"
            // Creating row data and inserting to vector
            _fpgaSWFuncCode += "    int nrow1 = " + tbl_in_1 + ".getNumRow();"
            _fpgaSWFuncCode += "    std::vector<" + sortRowName + "> rows;"
            _fpgaSWFuncCode += "    for (int i = 0; i < nrow1; i++) {"
            var rowString = ""
            var i = 0
            for (col <- _children.head.outputCols) {
              var input_col_type = getColumnType(col, dfmap)
              var input_col_symbol = stripColumnName(col)
              if (input_col_type == "IntegerType") {
                _fpgaSWFuncCode += "        int32_t " + input_col_symbol + " = " + tbl_in_1 + ".getInt32(i, " + i + ");"
                rowString += input_col_symbol + ","
              }
              else if (input_col_type == "LongType") {
                _fpgaSWFuncCode += "        int64_t " + input_col_symbol + " = " + tbl_in_1 + ".getInt64(i, " + i + ");"
                rowString += input_col_symbol + ","
              }
              else if (input_col_type == "DoubleType") {
                _fpgaSWFuncCode += "        int32_t " + input_col_symbol + " = " + tbl_in_1 + ".getInt32(i, " + i + ");"
                rowString += input_col_symbol + ","
              }
              else if (input_col_type == "StringType") {
                if (_stringRowIDBackSubstitution) {
                  //find the original stringRowID table that contains this string data
                  var orig_table_names = get_stringRowIDOriginalTableName(this)
                  var orig_table_columns = get_stringRowIDOriginalTableColumns(this)
                  var orig_tbl_idx = -1
                  var orig_tbl_col_idx = -1
                  for (orig_tbl <- orig_table_columns) {
                    for (orig_col <- orig_tbl) {
                      if (columnDictionary(col) == columnDictionary(orig_col)) {
                        orig_tbl_idx = orig_table_columns.indexOf(orig_tbl)
                        orig_tbl_col_idx = orig_tbl.indexOf(orig_col)
                      }
                    }
                  }
                  //find the col index of the string data in the original stringRowID table
                  if (orig_tbl_idx == -1 && orig_tbl_col_idx == -1) {
                    _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1> " + input_col_symbol + " = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1>(i, " + i + ");"
                  }
                  else {
                    var rowIDNum = tbl_in_1 + ".getInt32(i, " + i + ")"
                    _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1> " + input_col_symbol + " = " + orig_table_names(orig_tbl_idx) + ".getcharN<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1>(" + rowIDNum + ", " + orig_tbl_col_idx + ");"
                  }
                }
                else {
                  _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1> " + input_col_symbol + " = " + tbl_in_1 + ".getcharN<char, " + getStringLengthMacro(columnDictionary(col)) + " +1>(i, " + i + ");"
                }
                rowString += "std::string(" + input_col_symbol + ".data()),"
              } else {
                _fpgaSWFuncCode += "        // Unsupported input column type"
              }
              i += 1
            }
            _fpgaSWFuncCode += "        " + sortRowName + " t = {" + rowString.stripPrefix(",").stripSuffix(",") + "};"
            _fpgaSWFuncCode += "        rows.push_back(t);"
            _fpgaSWFuncCode += "    }"
            // Issuing the sorting call
            _fpgaSWFuncCode += "    std::sort(rows.begin(), rows.end(), " + sortLamdaFuncName + ");"
            // Writing sorted vector to output table
            _fpgaSWFuncCode += "    int r = 0;"
            _fpgaSWFuncCode += "    for (auto& it : rows) {"
            var printOutString = "            std::cout"
            i = 0
            for (col <- outputCols) {
              var output_col_type = getColumnType(col, dfmap)
              var formatted_col = stripColumnName(col)
              if (output_col_type == "IntegerType") {
                _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + i + ", it." + formatted_col + ");"
                printOutString += " << " + "it." + formatted_col + " << " + "\" \""
              }
              else if (output_col_type == "LongType") {
                _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt64(r, " + i + ", it." + formatted_col + ");"
                printOutString += " << " + "it." + formatted_col + " << " + "\" \""
              }
              else if (output_col_type == "DoubleType") {
                _fpgaSWFuncCode += "        " + tbl_out_1 + ".setInt32(r, " + i + ", it." + formatted_col + ");"
                printOutString += " << " + "it." + formatted_col + " << " + "\" \""
              }
              else if (output_col_type == "StringType") {
                _fpgaSWFuncCode += "        std::array<char, " + getStringLengthMacro(columnDictionary(col)) + " + 1> " + formatted_col + "{};"
                _fpgaSWFuncCode += "        memcpy(" + formatted_col + ".data(), (it." + formatted_col + ").data(), (it." + formatted_col + ").length());"
                _fpgaSWFuncCode += "        " + tbl_out_1 + ".setcharN<char, " + getStringLengthMacro(columnDictionary(col)) + " +1>(r, " + i + ", " + formatted_col + ");"
                printOutString += " << " + "(it." + formatted_col + ").data()" + " << " + "\" \""
              } else {
                _fpgaSWFuncCode += "        // Unsupported input column type"
              }
              i += 1
            }
            if (treeDepth == 0) {
              printOutString += " << std::endl;"
              _fpgaSWFuncCode += "        if (r < 10) {"
              _fpgaSWFuncCode += printOutString
              _fpgaSWFuncCode += "        }"
            }
            _fpgaSWFuncCode += "        ++r;"
            _fpgaSWFuncCode += "    }"
            _fpgaSWFuncCode += "    " + tbl_out_1 + ".setNumRow(r);"
          case _ =>
            var tempStr = "void " + _fpgaSWFuncName + "("
            for (ch <- _children){
              tempStr += "Table &" + ch.fpgaOutputTableName + ", "
            }
            tempStr += "Table &" + _fpgaOutputTableName + ") {"
            _fpgaSWFuncCode += tempStr
            _fpgaSWFuncCode += "    // Unsupported operation: " + _nodeType
        }
        _fpgaSWFuncCode += "    std::cout << \"" + _fpgaOutputTableName + " #Row: \" << " + _fpgaOutputTableName + ".getNumRow() << std::endl;"
        _fpgaSWFuncCode += "}"
      }
    }
    //-----------------------------------POST-PROCESSING----------------------------------------------
    if (_nodeType == "Aggregate" || _nodeType == "Project") {
      var groupByExists = false
      if (_groupBy_operation.length > 0) {
        groupByExists = true
      }
      if (groupByExists == false) {
        var tbl_out = _fpgaOutputTableName
        var op_idx = 0
        for (groupBy_payload <- _aggregate_expression) {
          var col_symbol = groupBy_payload.toString.split(" AS ").last
          var col_type = getColumnType(col_symbol, dfmap)
          var col_symbol_trimmed = stripColumnName(col_symbol)
          var col_aggregate_ops = getAggregateOperations(groupBy_payload.children(0))
          if (col_type == "IntegerType") {
          } else if (col_type == "LongType") {
          } else if (col_type == "StringType") {
            columnDictionary(col_symbol_trimmed) = columnDictionary(groupBy_payload.references.head.toString)
          } else if (col_type == "DoubleType") {
            columnDictionary(col_symbol) = ("LongType", "NULL")
          } else {
            col_type = col_type.split("\\(").head
            if (col_type == "DecimalType"){
              columnDictionary(col_symbol) = ("LongType", "NULL")
              col_type = "LongType"
            }
          }
          col_type = getColumnType(col_symbol, dfmap)
          if (col_type == "IntegerType") {
            //Alec-added: adding aggregation result based on the "AS" name in the case of subquery
            columnDictionary += (col_symbol_trimmed -> (tbl_out + ".getInt32(" + op_idx + ", 0)", "NULL"))
          } else if (col_type == "LongType") {
            //Alec-added: adding aggregation result based on the "AS" name in the case of subquery
            columnDictionary += (col_symbol_trimmed -> (tbl_out + ".getInt64(" + op_idx + ", 0)", "NULL"))
          } else {
          }
          op_idx += col_aggregate_ops.length
        }
      }
    }
    //-----------------------------------END-----------------------------------
  }

  def printSectionCode(sectionCode: ListBuffer[String], numIndent: Int): Unit ={
    if (sectionCode.nonEmpty){
      for (ln <- sectionCode){
        print_indent(numIndent)
        println(ln)
      }
      print("\n")
    }
  }

  def printHostCode: Unit ={
    for (ch <- _children) {
      ch.printHostCode
    }
    if (_operation.nonEmpty){
      print_indent(_treeDepth)
      println(_fpgaNodeName + " - " + _cpuORfpga)
      print("\n")
      printSectionCode(_fpgaInputCode, _treeDepth)
      printSectionCode(_fpgaOutputCode, _treeDepth)
      printSectionCode(_fpgaConfigCode, _treeDepth)
      printSectionCode(_fpgaKernelSetupCode, _treeDepth)
      printSectionCode(_fpgaTransEngineSetupCode, _treeDepth)
      printSectionCode(_fpgaKernelEventsCode, _treeDepth)
      printSectionCode(_fpgaKernelRunCode, _treeDepth)
      printSectionCode(_fpgaSWFuncCode, _treeDepth)
    }
  }

  def printKernelConfigCode: Unit ={

  }

  def printSWFuncCode: Unit ={

  }
}