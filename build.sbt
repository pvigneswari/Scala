ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.0"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"
// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.6"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3"

libraryDependencies += "org.postgresql" % "postgresql" % "42.5.0"

libraryDependencies += "com.typesafe" % "config" % "1.4.2"

//In build.sbt

scalaVersion := "2.12.10" // or "2.11.12" depending on your Spark version

lazy val root = (project in file("."))
  .settings(
    name := "SparkDemoJul",
    idePackagePrefix := Some("org.itc.com")
  )
