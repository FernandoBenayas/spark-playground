name := "Perform transformations on DataFrames"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2"
libraryDependencies += "joda-time" % "joda-time" % "2.10.10"
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.20.0"