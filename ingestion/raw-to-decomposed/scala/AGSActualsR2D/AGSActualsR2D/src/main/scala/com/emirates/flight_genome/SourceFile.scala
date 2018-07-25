package com.emirates.flight_genome

/**
 * Class for csvFile
 * @param path the full file path in Helix in String
 */
case class SourceFile(path: String) {

  private lazy val path_parts = path.split("/")
  lazy val fileName = path_parts(path_parts.length-1)
  lazy val folderName = path_parts(path_parts.length-2)

}

