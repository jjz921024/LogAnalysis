name := "LogAnalysis"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0",

  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0",

  /*"org.apache.spark" % "spark-core_2.11" % "2.0.2",
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.2",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.2",*/

  "com.mchange" % "c3p0" % "0.9.5.2",
  "mysql" % "mysql-connector-java" % "5.1.6"
)


