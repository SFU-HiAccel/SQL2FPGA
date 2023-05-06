package org.example
import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.{ListBuffer, Queue, Stack}

class SQL2FPGA_Codegen {
  //----------------------------------------------------------------------------------------------------------------
  // Code gen functions: SQL2FPGA AST -> c++ exection code
  //----------------------------------------------------------------------------------------------------------------
  def resetVisitTag(rootNode: SQL2FPGA_QPlan): Unit = {
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

  def writeHostCode_wrapper_top(root: SQL2FPGA_QPlan, bw: BufferedWriter, queryNo: Int, pure_sw_mode: Int, TPCH_or_DS: Int): Unit ={
    bw.write("#include <sys/time.h> \n")
    bw.write("#include <algorithm> \n")
    bw.write("#include <cstring> \n")
    bw.write("#include <fstream> \n")
    bw.write("#include <iomanip> \n")
    bw.write("#include <iostream> \n")
    bw.write("#include <sstream> \n")
    bw.write("#include <climits> \n")
    bw.write("#include <unordered_map> \n")
    bw.write("const int PU_NM = 8; \n")
    bw.write("\n")
    bw.write("#include \"table_dt.hpp\" \n")
    bw.write("#include \"utils.hpp\" \n")
    bw.write("#include \"tpch_read_2.hpp\" \n")
//    bw.write("#include \"tpcds_read.hpp\" \n")
    bw.write("#include \"gqe_api.hpp\" \n")
    bw.write("\n")
    bw.write("#include \"cfgFunc_q" + queryNo + ".hpp\" \n")
    bw.write("#include \"q" + queryNo + ".hpp\" \n")
    bw.write("\n")
    bw.write("int main(int argc, const char* argv[]) { \n")
    bw.write("    std::cout << \"\\n------------ TPC-H Query Test -------------\\n\"; \n")
    bw.write("    ArgParser parser(argc, argv); \n")
    if (pure_sw_mode != 1) {
      bw.write("    std::string xclbin_path; \n")
      bw.write("    if (!parser.getCmdOption(\"-xclbin\", xclbin_path)) { \n")
      bw.write("        std::cout << \"ERROR: xclbin path is not set!\\n\"; \n")
      bw.write("        return 1; \n")
      bw.write("    } \n")
      //gqe-join
      bw.write("    std::string xclbin_path_h; \n")
      bw.write("    if (!parser.getCmdOption(\"-xclbin_h\", xclbin_path_h)) { \n")
      bw.write("        std::cout << \"ERROR: xclbin_h path is not set!\\n\"; \n")
      bw.write("        return 1; \n")
      bw.write("    } \n")
      //gqe-aggr
      bw.write("    std::string xclbin_path_a; \n")
      bw.write("    if (!parser.getCmdOption(\"-xclbin_a\", xclbin_path_a)) { \n")
      bw.write("        std::cout << \"ERROR: xclbin_a path is not set!\\n\"; \n")
      bw.write("        return 1; \n")
      bw.write("    } \n")
    }
    bw.write("    std::string in_dir; \n")
    bw.write("    if (!parser.getCmdOption(\"-in\", in_dir) || !is_dir(in_dir)) { \n")
    bw.write("        std::cout << \"ERROR: input dir is not specified or not valid.\\n\"; \n")
    bw.write("        return 1; \n")
    bw.write("    } \n")

    bw.write("    int num_rep = 1; \n")
    bw.write("    std::string num_str; \n")
    bw.write("    if (parser.getCmdOption(\"-rep\", num_str)) { \n")
    bw.write("        try { \n")
    bw.write("            num_rep = std::stoi(num_str); \n")
    bw.write("        } catch (...) { \n")
    bw.write("            num_rep = 1; \n")
    bw.write("        } \n")
    bw.write("    } \n")
    bw.write("    if (num_rep > 20) { \n")
    bw.write("        num_rep = 20; \n")
    bw.write("        std::cout << \"WARNING: limited repeat to \" << num_rep << \" times\\n.\"; \n")
    bw.write("    } \n")

    bw.write("    int scale = 1; \n")
    bw.write("    std::string scale_str; \n")
    bw.write("    if (parser.getCmdOption(\"-c\", scale_str)) { \n")
    bw.write("        try { \n")
    bw.write("            scale = std::stoi(scale_str); \n")
    bw.write("        } catch (...) { \n")
    bw.write("            scale = 1; \n")
    bw.write("        } \n")
    bw.write("    } \n")

    bw.write("    std::cout << \"NOTE:running query #" + queryNo + "\\n.\"; \n")
    bw.write("    std::cout << \"NOTE:running in sf\" << scale << \" data\\n\"; \n\n")

    if (TPCH_or_DS == 0) {
      //Alec-added TPCH
      bw.write("    int32_t lineitem_n = SF1_LINEITEM; \n")
      bw.write("    int32_t supplier_n = SF1_SUPPLIER; \n")
      bw.write("    int32_t nation_n = SF1_NATION; \n")
      bw.write("    int32_t order_n = SF1_ORDERS; \n")
      bw.write("    int32_t customer_n = SF1_CUSTOMER; \n")
      bw.write("    int32_t region_n = SF1_REGION; \n")
      bw.write("    int32_t part_n = SF1_PART; \n")
      bw.write("    int32_t partsupp_n = SF1_PARTSUPP; \n")
      bw.write("    if (scale == 30) { \n")
      bw.write("        lineitem_n = SF30_LINEITEM; \n")
      bw.write("        supplier_n = SF30_SUPPLIER; \n")
      bw.write("        nation_n = SF30_NATION; \n")
      bw.write("        order_n = SF30_ORDERS; \n")
      bw.write("        customer_n = SF30_CUSTOMER; \n")
      bw.write("        region_n = SF30_REGION; \n")
      bw.write("        part_n = SF30_PART; \n")
      bw.write("        partsupp_n = SF30_PARTSUPP; \n")
      bw.write("    } \n")
    } else if (TPCH_or_DS == 1) {
      //Alec-added TPCDS
      bw.write("    int32_t customer_n = 	100000; \n")
      bw.write("    int32_t customer_address_n = 	50000; \n")
      bw.write("    int32_t customer_demographics_n = 	1920800; \n")
      bw.write("    int32_t date_dim_n = 	73049; \n")
      bw.write("    int32_t household_demographics_n = 	7200; \n")
      bw.write("    int32_t income_band_n = 	20; \n")
      bw.write("    int32_t item_n = 	18000; \n")
      bw.write("    int32_t promotion_n = 	300; \n")
      bw.write("    int32_t reason_n = 	35; \n")
      bw.write("    int32_t ship_mode_n = 	20; \n")
      bw.write("    int32_t store_n = 	12; \n")
      bw.write("    int32_t time_dim_n = 	86400; \n")
      bw.write("    int32_t warehouse_n = 	5; \n")
      bw.write("    int32_t web_site_n = 	30; \n")
      bw.write("    int32_t web_page_n = 	60; \n")
      bw.write("    int32_t inventory_n = 	11745000; \n")
      bw.write("    int32_t store_returns_n = 	287514; \n")
      bw.write("    int32_t web_sales_n = 	719384; \n")
      bw.write("    int32_t web_returns_n = 	71763; \n")
      bw.write("    int32_t call_center_n = 	6; \n")
      bw.write("    int32_t catalog_page_n = 	11718; \n")
      bw.write("    int32_t catalog_returns_n = 	144067; \n")
      bw.write("    int32_t catalog_sales_n = 	1441548; \n")
      bw.write("    int32_t store_sales_n = 	2880404    ; \n")
    }
    bw.write("    // ********************************************************** // \n")
  }
  def writeHostCode_section(l: ListBuffer[String], bw: BufferedWriter): Unit ={
    if (l.nonEmpty){
      for (ln <- l){
        bw.write("    " + ln + "\n")
      }
    }
  }
  def isDescdentOfNonUniqueNode(root: SQL2FPGA_QPlan): Boolean ={
    var result = false
    // check if 'ch' is a descendant of a non-unique node
    var descendantOfRepeatedNode = false
    var iterNode = root
    if (iterNode.parent.nonEmpty) {
      while (iterNode != null && !descendantOfRepeatedNode) {
        if (iterNode.parent.nonEmpty && !iterNode.isUniqueNode(iterNode.parent(0))) {
          descendantOfRepeatedNode = true
          return descendantOfRepeatedNode
        } else {
          if (iterNode.parent.nonEmpty) {
            iterNode = iterNode.parent(0)
          } else {
            iterNode = null
          }

        }
      }
    }
    return result
  }
  def writeHostCode_tables_inoutTbl_definition(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)

    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to file buffer
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      writeHostCode_section(tmp_node.fpgaOutputCode, bw)
    }
  }
  def writeHostCode_tables_inoutTbl_allocateDevBuffer(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)

    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to file buffer
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      writeHostCode_section(tmp_node.fpgaOutputDevAllocateCode, bw)
    }
  }
  def writeHostCode_tables(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    writeHostCode_tables_inoutTbl_definition(parent, root, bw)
    resetVisitTag(root)
    bw.write("    // ********************** Allocate Device Buffer ******************** // \n")
    writeHostCode_tables_inoutTbl_allocateDevBuffer(parent, root, bw)
    resetVisitTag(root)
  }
  def writeHostCode_config(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)

    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to file buffer
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      writeHostCode_section(tmp_node.fpgaConfigCode, bw)
    }
  }
  def writeHostCode_kernelSetup(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)

    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to file buffer
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      writeHostCode_section(tmp_node.fpgaKernelSetupCode, bw)
    }
  }
  def writeHostCode_transferEngine(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)

    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to file buffer
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      writeHostCode_section(tmp_node.fpgaTransEngineSetupCode, bw)
    }
  }
  def writeHostCode_events(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)

    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to file buffer
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      writeHostCode_section(tmp_node.fpgaKernelEventsCode, bw)
    }
  }
  def writeHostCode_operation(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    //    for (ch <- root.children) {
    //      writeHostCode_operation(root, ch, bw)
    //    }
    //    if (root.operation.nonEmpty){
    //      var timeVal_id = scala.util.Random.nextInt(1000)
    //      bw.write("    struct timeval " + "tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_s, " + "tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_e;" + "\n")
    //      bw.write("    gettimeofday(&" + "tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_s, 0);" + "\n")
    //      if (root.cpuORfpga == 0) { // CPU SW implementation
    //        writeHostCode_section(root.fpgaSWCode, bw)
    //      }
    //      else if (root.cpuORfpga == 1) { // FPGA HW implementation
    //        writeHostCode_section(root.fpgaKernelRunCode, bw)
    //      }
    //      bw.write("    gettimeofday(&" + "tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_e, 0);" + "\n\n")
    //
    //      // root.executionTimeCode += "print_h_time(tv_r_s, " + "tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_s, " + "tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_e, " + "\"" + root.nodeType + "_" + root.treeDepth.toString + "\"); std::cout << std::endl;"
    //      root.executionTimeCode += "std::cout << \"" + root.nodeType + "_" + root.treeDepth.toString + ": \" << tvdiff(&tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_s, " + "&tv_r_" + root.nodeType + "_" + root.treeDepth.toString + "_" + timeVal_id + "_e) / 1000.0 << \" ms \" "
    //      var tbl_size_str = " << "
    //      for (ch <- root.children) {
    //        tbl_size_str += "\"" + ch.fpgaOutputTableName + ": \" << " + ch.fpgaOutputTableName + ".getNumRow() << \" \" << "
    //      }
    //      tbl_size_str += "std::endl; \n"
    //      root.executionTimeCode += tbl_size_str
    //    }
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)
    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to stack to file buffer
    var s_entire_plan = Stack[SQL2FPGA_QPlan]()
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      s_entire_plan.push(tmp_node)
    }
    //Final write out to file buffer
    while(s_entire_plan.nonEmpty) {
      var tmp_node = s_entire_plan.pop()
      if (tmp_node.operation.nonEmpty){
        var timeVal_id = scala.util.Random.nextInt(1000)
        bw.write("    struct timeval " + "tv_r_" + tmp_node.nodeType + "_" + tmp_node.treeDepth.toString + "_" + timeVal_id + "_s, " + "tv_r_" + tmp_node.nodeType + "_" + tmp_node.treeDepth.toString + "_" + timeVal_id + "_e;" + "\n")
        bw.write("    gettimeofday(&" + "tv_r_" + tmp_node.nodeType + "_" + tmp_node.treeDepth.toString + "_" + timeVal_id + "_s, 0);" + "\n")
        if (tmp_node.cpuORfpga == 0) { // CPU SW implementation
          writeHostCode_section(tmp_node.fpgaSWCode, bw)
        }
        else if (tmp_node.cpuORfpga == 1) { // FPGA HW implementation
          writeHostCode_section(tmp_node.fpgaKernelRunCode, bw)
        }
        bw.write("    gettimeofday(&" + "tv_r_" + tmp_node.nodeType + "_" + tmp_node.treeDepth.toString + "_" + timeVal_id + "_e, 0);" + "\n\n")

        tmp_node.executionTimeCode += "std::cout << \"" + tmp_node.nodeType + "_" + tmp_node.treeDepth.toString + ": \" << tvdiff(&tv_r_" + tmp_node.nodeType + "_" + tmp_node.treeDepth.toString + "_" + timeVal_id + "_s, " + "&tv_r_" + tmp_node.nodeType + "_" + tmp_node.treeDepth.toString + "_" + timeVal_id + "_e) / 1000.0 << \" ms \" "
        var tbl_size_str = " << "
        for (ch <- tmp_node.children) {
          tbl_size_str += "\"" + ch.fpgaOutputTableName + ": \" << " + ch.fpgaOutputTableName + ".getNumRow() << \" \" << "
        }
        tbl_size_str += "std::endl; \n"
        tmp_node.executionTimeCode += tbl_size_str
      }
    }

  }
  def writeHostCode_executionTime(parent: SQL2FPGA_QPlan, root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)
    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to stack to file buffer
    var s_entire_plan = Stack[SQL2FPGA_QPlan]()
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      s_entire_plan.push(tmp_node)
    }
    //Final write out to file buffer
    while(s_entire_plan.nonEmpty) {
      var tmp_node = s_entire_plan.pop()
      writeHostCode_section(tmp_node.executionTimeCode, bw)
    }
  }
  def writeHostCode_Core(root: SQL2FPGA_QPlan, bw: BufferedWriter, pure_sw_mode: Int, sf: Int): Unit ={
    if (pure_sw_mode != 1) {
      bw.write("    // Get CL devices. \n")
      // TODO: assume we have 2 FPGAs, what kind of overlay (gqe-join or gqe-aggr) should occupy the FPGAs?
      bw.write("    std::vector<cl::Device> devices = xcl::get_xil_devices(); \n")
      if (root.fpgaJoinOverlayCallCount > 0) {
        bw.write("    cl::Device device_h = devices[0]; \n")
        bw.write("    // Create context_h and command queue for selected device \n")
        bw.write("    cl::Context context_h(device_h); \n")
        bw.write("    cl::CommandQueue q_h(context_h, device_h, CL_QUEUE_PROFILING_ENABLE | CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE); \n")
        bw.write("    std::string devName_h = device_h.getInfo<CL_DEVICE_NAME>(); \n")
        bw.write("    std::cout << \"Selected Device \" << devName_h << \"\\n\"; \n")
        bw.write("    cl::Program::Binaries xclBins_h = xcl::import_binary_file(xclbin_path_h); \n")
        bw.write("    std::vector<cl::Device> devices_h; \n")
        bw.write("    devices_h.push_back(device_h); \n")
        bw.write("    cl::Program program_h(context_h, devices_h, xclBins_h); \n")
      }
      if (root.fpgaAggrOverlayCallCount > 0) {
        bw.write("    cl::Device device_a = devices[1]; \n")
        bw.write("    // Create context_a and command queue for selected device \n")
        bw.write("    cl::Context context_a(device_a); \n")
        bw.write("    cl::CommandQueue q_a(context_a, device_a, CL_QUEUE_PROFILING_ENABLE | CL_QUEUE_OUT_OF_ORDER_EXEC_MODE_ENABLE); \n")
        bw.write("    std::string devName_a = device_a.getInfo<CL_DEVICE_NAME>(); \n")
        bw.write("    std::cout << \"Selected Device \" << devName_a << \"\\n\"; \n")
        bw.write("    cl::Program::Binaries xclBins_a = xcl::import_binary_file(xclbin_path_a); \n")
        bw.write("    std::vector<cl::Device> devices_a; \n")
        bw.write("    devices_a.push_back(device_a); \n")
        bw.write("    cl::Program program_a(context_a, devices_a, xclBins_a); \n")
      }
    }
    if (sf == 30) {
      bw.write("    // *********************** Partition Infomation ********************* // \n")
      bw.write("    int hpTimes_join = 32; \n")
      bw.write("    int power_of_hpTimes_join = log2(hpTimes_join); \n")
      bw.write("    std::cout << \"Number of partition (gqeJoin) is: \" << hpTimes_join << std::endl; \n")
      bw.write("    int hpTimes_aggr = 256; \n")
      bw.write("    int power_of_hpTimes_aggr = log2(hpTimes_aggr); \n")
      bw.write("    std::cout << \"Number of partition (gqeAggr) is: \" << hpTimes_aggr << std::endl; \n")
    }
    bw.write("    // ****************************** Tables **************************** // \n")
    writeHostCode_tables(null, root, bw)
    resetVisitTag(root)
    bw.write("    // ****************************** Config **************************** // \n")
    writeHostCode_config(null, root, bw)
    resetVisitTag(root)
    bw.write("    // *************************** Kernel Setup ************************* // \n")
    if (pure_sw_mode != 1) {
      // gqe-join
      if (root.fpgaJoinOverlayCallCount > 0) {
        bw.write("    bufferTmp buftmp_h(context_h); \n")
        bw.write("    buftmp_h.initBuffer(q_h); \n")
      }
      // gqe-aggr
      if (root.fpgaAggrOverlayCallCount > 0) {
        bw.write("    AggrBufferTmp buftmp_a(context_a); \n")
        bw.write("    buftmp_a.BufferInitial(q_a); \n")
      }
      bw.write("    std::cout << std::endl; \n")
    }
    writeHostCode_kernelSetup(null, root, bw)
    resetVisitTag(root)
    bw.write("    // ************************** Transfer Engine *********************** // \n")
    writeHostCode_transferEngine(null, root, bw)
    resetVisitTag(root)
    bw.write("    // ****************************** Events **************************** // \n")
    writeHostCode_events(null, root, bw)
    resetVisitTag(root)
    bw.write("    // **************************** Operations ************************** // \n")
    bw.write("    struct timeval tv_r_s, tv_r_e; \n")
    bw.write("    gettimeofday(&tv_r_s, 0); \n\n")
    writeHostCode_operation(null, root, bw)
    resetVisitTag(root)
    bw.write("    gettimeofday(&tv_r_e, 0); \n")
    bw.write("    // **************************** Print Execution Time ************************** // \n")
    writeHostCode_executionTime(null, root, bw)
    resetVisitTag(root)
    bw.write("    std::cout << std::endl << \" Total execution time: \" << tvdiff(&tv_r_s, &tv_r_e) / 1000 << \" ms\"; \n\n")
  }
  def writeHostCode_wrapper_bottom(bw: BufferedWriter, goldenOutput: ListBuffer[String]): Unit ={
    bw.write("    std::cout << std::endl << \" Spark elapsed time: \" << " + goldenOutput.last + " * 1000 << \"ms\" << std::endl; \n")
//    bw.write("    std::cout << \" Spark Output (first 5 of " + (goldenOutput.length - 1).toString  + " lines)\" << std::endl; \n")
//    var idx = 0
//    for (line <- goldenOutput) {
//      if (idx < 5) {
//        bw.write("    std::cout << \"" + line + "\" << std::endl; \n")
//      }
//      idx += 1
//    }
    bw.write("    return 0; \n")
    bw.write("}\n")
  }
  def writeFPGAConfigCode_header(bw: BufferedWriter): Unit ={
    bw.write("#include \"ap_int.h\" \n")
    bw.write("#include \"xf_database/dynamic_alu_host.hpp\"\n")
    bw.write("#include \"xf_database/enums.hpp\"\n")
    bw.write("#include <fstream> \n")
    bw.write(" \n")
    bw.write("static void gen_pass_fcfg(uint32_t cfg[]) { \n")
    bw.write("    using namespace xf::database; \n")
    bw.write("    int n = 0; \n")
    bw.write(" \n")
    bw.write("    // cond_1 \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (FOP_DC); \n")
    bw.write("    // cond_2 \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (FOP_DC); \n")
    bw.write("    // cond_3 \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (FOP_DC); \n")
    bw.write("    // cond_4 \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = 0UL | (FOP_DC << FilterOpWidth) | (FOP_DC); \n")
    bw.write(" \n")
    bw.write("    uint32_t r = 0; \n")
    bw.write("    int sh = 0; \n")
    bw.write("    // cond_1 -- cond_2 \n")
    bw.write("    r |= ((uint32_t)(FOP_DC << sh)); \n")
    bw.write("    sh += FilterOpWidth; \n")
    bw.write("    // cond_1 -- cond_3 \n")
    bw.write("    r |= ((uint32_t)(FOP_DC << sh)); \n")
    bw.write("    sh += FilterOpWidth; \n")
    bw.write("    // cond_1 -- cond_4 \n")
    bw.write("    r |= ((uint32_t)(FOP_DC << sh)); \n")
    bw.write("    sh += FilterOpWidth; \n")
    bw.write(" \n")
    bw.write("    // cond_2 -- cond_3 \n")
    bw.write("    r |= ((uint32_t)(FOP_DC << sh)); \n")
    bw.write("    sh += FilterOpWidth; \n")
    bw.write("    // cond_2 -- cond_4 \n")
    bw.write("    r |= ((uint32_t)(FOP_DC << sh)); \n")
    bw.write("    sh += FilterOpWidth; \n")
    bw.write(" \n")
    bw.write("    // cond_3 -- cond_4 \n")
    bw.write("    r |= ((uint32_t)(FOP_DC << sh)); \n")
    bw.write("    sh += FilterOpWidth; \n")
    bw.write(" \n")
    bw.write("    cfg[n++] = r; \n")
    bw.write(" \n")
    bw.write("    // 4 true and 6 true \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)0UL; \n")
    bw.write("    cfg[n++] = (uint32_t)(1UL << 31); \n")
    bw.write("} \n\n")
  }
  def writeFPGAConfigCode_section(l: ListBuffer[String], bw: BufferedWriter): Unit ={
    if (l.nonEmpty){
      for (ln <- l){
        bw.write(ln + "\n")
      }
    }
    bw.write("\n");
  }
  def writeFPGAConfigCode_Core(root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)
    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to stack to file buffer
    var s_entire_plan = Stack[SQL2FPGA_QPlan]()
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      s_entire_plan.push(tmp_node)
    }
    //Final write out to file buffer
    while(s_entire_plan.nonEmpty) { // both HW and SW operator can generate SWFuncCode - gqe-aggr
      var tmp_node = s_entire_plan.pop()
      if (tmp_node.operation.nonEmpty && tmp_node.cpuORfpga == 1){
        writeFPGAConfigCode_section(tmp_node.fpgaConfigFuncCode, bw)
      }
    }
  }
  def writeSWConfigCode_header(bw: BufferedWriter): Unit ={
    bw.write("#include <regex> \n")
    bw.write("#include <stdint.h> \n")
    bw.write("\n")
  }
  def writeSWConfigCode_section(l: ListBuffer[String], bw: BufferedWriter): Unit ={
    if (l.nonEmpty){
      for (ln <- l){
        bw.write(ln + "\n")
      }
    }
    bw.write("\n");
  }
  def writeSWConfigCode_Core(root: SQL2FPGA_QPlan, bw: BufferedWriter): Unit ={
    var q_entire_plan = Queue[SQL2FPGA_QPlan]()
    q_entire_plan.enqueue(root)
    //Breadth traversal queue
    var q_node = Queue[SQL2FPGA_QPlan]()
    q_node.enqueue(root)

    while(!q_node.isEmpty) {
      // traversal step
      var this_node = q_node.dequeue()
      for (ch <- this_node.children) {
        q_node.enqueue(ch)
      }
      // update entire plan queue
      var tmp_q = Queue[SQL2FPGA_QPlan]()
      for (ch <- this_node.children) {
        if (q_entire_plan.contains(ch)) {
          tmp_q.clear()
          while(!q_entire_plan.isEmpty) {
            var tmp_node = q_entire_plan.dequeue()
            if (tmp_node != ch) {
              tmp_q.enqueue(tmp_node)
            }
          }
          tmp_q.enqueue(ch)
          q_entire_plan = tmp_q.clone()
        } else {
          q_entire_plan.enqueue(ch)
        }
      }
    }
    //Final write out to stack to file buffer
    var s_entire_plan = Stack[SQL2FPGA_QPlan]()
    while(q_entire_plan.nonEmpty) {
      var tmp_node = q_entire_plan.dequeue()
      s_entire_plan.push(tmp_node)
    }
    //Final write out to file buffer
    while(s_entire_plan.nonEmpty) {
      var tmp_node = s_entire_plan.pop()
      // if (tmp_node.operation.nonEmpty && tmp_node.cpuORfpga == 0){ // only SW operator can generate SWFuncCode
      if (tmp_node.operation.nonEmpty) { // both HW and SW operator can generate SWFuncCode - gqe-aggr
        writeSWConfigCode_section(tmp_node.fpgaSWFuncCode, bw)
      }
    }
  }
  def genHostCode(root: SQL2FPGA_QPlan, pure_sw_mode: Int, num_fpga_device: Int, queryNo: Int, goldenOutput: ListBuffer[String], sf: Int, num_overlay_orig: Int, num_overlay_fused: Int, TPCH_or_DS: Int): Unit ={
    // host.cpp
    var hostCodeFileName = "test_q" + queryNo + ".cpp"
    val outFile = new File(hostCodeFileName)
    val bw = new BufferedWriter(new FileWriter(outFile, false))
    bw.write("// number of overlays (w/o fusion): " + num_overlay_orig.toString + " \n")
    bw.write("// number of overlays (w/ fusion): " + num_overlay_fused.toString + " \n")
    writeHostCode_wrapper_top(root, bw, queryNo, pure_sw_mode, TPCH_or_DS)
    writeHostCode_Core(root, bw, pure_sw_mode, sf)
    writeHostCode_wrapper_bottom(bw, goldenOutput)
    bw.close()
  }
  def genFPGAConfigCode(root: SQL2FPGA_QPlan, queryNo: Int, sf: Int): Unit ={
    // cfgFunc.hpp
    var cfgFuncFileName = "cfgFunc_q" + queryNo + ".hpp"
    val outFile = new File(cfgFuncFileName)
    val bw = new BufferedWriter(new FileWriter(outFile, false))
    writeFPGAConfigCode_header(bw)
    writeFPGAConfigCode_Core(root, bw)
    resetVisitTag(root)
    bw.close()
  }
  def genSWConfigCode(root: SQL2FPGA_QPlan, queryNo: Int, sf: Int): Unit ={
    // swFunc.hpp
    var swFuncFileName = "q" + queryNo + ".hpp"
    val outFile = new File(swFuncFileName)
    val bw = new BufferedWriter(new FileWriter(outFile, false))
    writeSWConfigCode_header(bw)
    writeSWConfigCode_Core(root, bw)
    resetVisitTag(root)
    bw.close()
  }
  def writeSparkSQLEvaluationResults(queryNo: Int, sf: Int, runtime_s: Float, bw: BufferedWriter): Unit = {
    bw.write(queryNo.toString + "(sf"+ sf.toString +"): " + runtime_s.toString + " s\n")
  }
}
