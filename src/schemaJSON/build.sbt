name := "Read JSON using defined schema"

version := "1.0"

scalaVersion := "2.12.10"

resolvers += "SynapseML" at "https://mmlspark.azureedge.net/maven"

libraryDependencies += "com.microsoft.azure" %% "synapseml" % "0.9.5-13-d1b51517-SNAPSHOT"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "joda-time" % "joda-time" % "2.10.10"
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.20.0"