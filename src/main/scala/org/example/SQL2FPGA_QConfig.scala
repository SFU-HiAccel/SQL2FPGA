package org.example
import scala.collection.mutable.ListBuffer

//----------------------------------------------------------------------------------------------------------------
// SQL2FPGA TPC-H query compiler configurations
//----------------------------------------------------------------------------------------------------------------
class SQL2FPGA_QConfig{
  // definitions
  private var _tpch_queryNum_start: Int = 5
  private var _tpch_queryNum_end: Int = 5
  private var _tpch_queryNum_list: ListBuffer[Int] = new ListBuffer[Int]()
  private var _tpcds_queryNum_start: Int = 1
  private var _tpcds_queryNum_end: Int = 1
  private var _tpcds_queryNum_list: ListBuffer[Int] = new ListBuffer[Int]()
  private var _num_fpga_device: Int = 0
  private var _pure_sw_mode: Int = 0
  private var _scale_factor: Int = 0
  private var _num_spark_execution: Int = 1
  private var _query_plan_optimization_enable: String = "00000" // 5 digit for 5 optimizations
  // getters
  def tpch_queryNum_start = _tpch_queryNum_start
  def tpch_queryNum_end = _tpch_queryNum_end
  def tpch_queryNum_list = _tpch_queryNum_list
  def tpcds_queryNum_start = _tpcds_queryNum_start
  def tpcds_queryNum_end = _tpcds_queryNum_end
  def tpcds_queryNum_list = _tpcds_queryNum_list
  def num_fpga_device = _num_fpga_device
  def pure_sw_mode = _pure_sw_mode
  def scale_factor = _scale_factor
  def num_spark_execution = _num_spark_execution
  def query_plan_optimization_enable = _query_plan_optimization_enable
  // setters
  def tpch_queryNum_start_= (newValue: Int): Unit = {
    _tpch_queryNum_start = newValue
  }
  def tpch_queryNum_end_= (newValue: Int): Unit = {
    _tpch_queryNum_end = newValue
  }
  def tpch_queryNum_list_= (newValue: ListBuffer[Int]): Unit = {
    _tpch_queryNum_list = newValue
  }
  def tpcds_queryNum_start_= (newValue: Int): Unit = {
    _tpcds_queryNum_start = newValue
  }
  def tpcds_queryNum_end_= (newValue: Int): Unit = {
    _tpcds_queryNum_end = newValue
  }
  def tpcds_queryNum_list_= (newValue: ListBuffer[Int]): Unit = {
    _tpcds_queryNum_list = newValue
  }
  def num_fpga_device_= (newValue: Int): Unit = {
    _num_fpga_device = newValue
  }
  def pure_sw_mode_= (newValue: Int): Unit = {
    _pure_sw_mode = newValue
  }
  def scale_factor_= (newValue: Int): Unit = {
    _scale_factor = newValue
  }
  def num_spark_execution_= (newValue: Int): Unit = {
    _num_spark_execution = newValue
  }
  def query_plan_optimization_enable_= (newValue: String): Unit = {
    _query_plan_optimization_enable = newValue
  }
}

