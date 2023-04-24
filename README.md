# SQL2FPGA: Automatic Acceleration of SQL Query Processing on Modern CPU-FPGA Platforms

This repository includes the code for SQL2FPGA. SQL2FPGA is a hardware-aware SQL query compilation framwork for translating and efficiently mapping SQL queries on the modern heterogeneous CPU-FPGA platforms. SQL2FPGA takes the optimized query execution plans of SQL queries from big data query processing engines; performs hardware-aware optimizations to map query operations to FPGA accelerators; and lastly generates the deployable CPU host code and the associated FPGA accelerator configuration code. 

If you find this project useful in your research, please consider citing:

    @inproceedings{lu2023sql2fpga,
      title={SQL2FPGA: Automatic Acceleration of SQL Query Processing on Modern CPU-FPGA Platforms},
      author={Lu, Alec and Fang, Zhenman},
      booktitle={Proceedings of the 31st IEEE International Symposium On Field-Programmable Custom Computing Machines},
      year={2023}
    }

## Download SQL2FPGA
        git clone https://github.com/SFU-HiAccel/SQL2FPGA.git

## Environmental Setup
1. **Hardware platforms (evaluated):**
    * **Host CPU:**
      * 64-bit Ubuntu 18.04.2 LTS
    * **FPGA Accelerator Design:
      * [Xilinx Database Library 2020.1](https://github.com/Xilinx/Vitis_Libraries/tree/2020.1/database)
    * **Cloud FPGA:**
      * Xilinx Alveo U280

2. **Software tools (evaluated):**
    * **Big data query processing tool:**
      * Spark 3.1.1
    * **HLS tool:**
      * Vitis 2020.1
      * Xilinx Runtime(XRT) 2020.1

## Accelerate SQL Query Processing using SQL2FPGA
1. Import SQL2FPGA Project using IntelliJ
2. Run SQL2FPGA Project on TPC-H Dataset
    * Dataset File Path
    * Query Specifications
    * Execution Mode
    * Output:
       * CPU Host Code:
       * FPGA Configuration Code:
       * SW Operator Function Code: 
3. Deploy Generated Design on Device
    * Move generated code to ``
    * Run:

Now you have compeletd the entire tool flow of SQL2FPGA. Hack the code and have fun!

## Authors and Contributors
SQL2FPGA is currently maintained by [Alec Lu](http://www.sfu.ca/~fla30/).

Besides, we thank AMD-Xilinx Vitis DB team, Prof. Jiannan Wang and Dr. Jinglin Peng from Simon Fraser University, for their insightful discussion and technical support.

## Papers
More implementation details of SQL2FPGA are covered in [our paper](http://www.sfu.ca/~fla30/papers/C9_FCCM_2023_SQL2FPGA.pdf).
