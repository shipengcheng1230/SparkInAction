name := "SparkInAction"

version := "0.1"

scalaVersion := "2.12.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"

// https://mvnrepository.com/artifact/com.databricks/spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.10.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-avro
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.0.0"

// https://mvnrepository.com/artifact/com.drewnoakes/metadata-extractor
libraryDependencies += "com.drewnoakes" % "metadata-extractor" % "2.14.0"

// https://mvnrepository.com/artifact/io.delta/delta-core
libraryDependencies += "io.delta" %% "delta-core" % "0.7.0"

// use "provided" scope to build uber jar
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided"

// using below will build a really fat uber jar
//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}