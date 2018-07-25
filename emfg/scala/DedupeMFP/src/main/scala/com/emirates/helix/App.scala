package com.emirates.helix

/**
 * @author ${user.name}
 */
object DedupMfpMain {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    val base_Input_Path = args(0).trim();
    val incr_Input_path = args(1).trim();
    val hist_Output_Path = args(2).trim();
    val inct_Output_Path= args(3).trim();
    val incr_Output_Path_Temp = args(4).trim();
    val loadType = args(5).trim();
    val months= args(6).trim().toInt;
    DedupMfpData.decompdataprocess(base_Input_Path: String, incr_Input_path: String, hist_Output_Path: String, inct_Output_Path: String, incr_Output_Path_Temp: String, loadType: String, months: Integer);
  }

}
