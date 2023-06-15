

scalaVersion := "2.12.11"



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1",
  //"junit" % "junit" % "4.8.1" % Test,
  "org.scalatest" %% "scalatest" % "3.2.1" % Test,
  "com.typesafe" % "config" % "1.4.2"
)