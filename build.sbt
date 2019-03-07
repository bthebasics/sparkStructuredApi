name := "sparkStructuredApi"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

resolvers += "graphframes" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

// https://mvnrepository.com/artifact/graphframes/graphframes

resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))
libraryDependencies += "graphframes" % "graphframes" % "0.6.0-spark2.2-s_2.11"

// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.0"

// https://mvnrepository.com/artifact/com.twosigma/flint
libraryDependencies += "com.twosigma" % "flint" % "0.6.0"


// https://mvnrepository.com/artifact/com.databricks/spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" %   "1.7.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3" excludeAll(
  ExclusionRule("com.amazonaws", "aws-java-sdk"),
  ExclusionRule("commons-beanutils")
)
    