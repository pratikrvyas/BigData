package com.emirates.cartrawlerrawtodecomp

/**
 * @author ${Sandeep Sunkavalli}
 */
object AppCarTraw {

  def main(args: Array[String]) {
    val process_date = args(0).trim();
    val basePathSource = args(1).trim();
    val basePathTarget = args(2).trim();
    val cartrawAptTrans = args(3).trim();
    val cartrawPaxDrop = args(4).trim();
    rawDatatoDecompTransfer.carTrawRawToDecomp(process_date, basePathSource, basePathTarget, cartrawAptTrans);
    rawDatatoDecompTransfer.carTrawRawToDecomp(process_date, basePathSource, basePathTarget, cartrawPaxDrop);
  }

}
