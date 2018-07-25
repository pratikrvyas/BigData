package com.emirates.helix

/**
 * @author ${user.name}
 */
object RawToDecompEpicIntRaw {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    val basePathSourceInit = args(0).trim();
    val basePathSourceIncr = args(1).trim();
    val basePathTargetInit = args(2).trim();
    val basePathTargetIncr = args(3).trim();
    val loadType = args(4).trim();
    RawToDecomTransfer.carTrawRawToDecomp( basePathSourceInit: String, basePathSourceIncr: String, basePathTargetInit: String, basePathTargetIncr: String, loadType: String)
  }

}
