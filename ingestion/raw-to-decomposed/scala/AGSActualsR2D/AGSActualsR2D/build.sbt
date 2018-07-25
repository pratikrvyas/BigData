name := "AGSActualsR2D"

version := "0.1"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  "emirates-repo" at "http://repository.emirates.com/repository/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-assembly_2.10" %"1.6.0-cdh5.8.2",
  "org.apache.spark" % "spark-yarn_2.10" % "1.6.0-cdh5.8.2",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.0-cdh5.8.2",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"
)



