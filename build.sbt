name := "small-spark"
version := "0.1"
scalaVersion := "2.11.12"

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  Resolver.jcenterRepo
)
libraryDependencies ++= {
  Seq(
    //@formatter:off
    "org.apache.spark" %% "spark-sql"  % "2.4.5" % Provided,
    "org.apache.hadoop" % "hadoop-aws" % "3.3.0" % Provided,
    "com.github.scopt" %% "scopt"      % "3.7.1"
    //@formatter:on
  )
}
// in case you have a higher version of jackson-databind in your code, add the following:
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

/*
 * dont forget to add the following line to project/assembly.sbt:
 * addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
 */
assemblyMergeStrategy in assembly := {
  //@formatter:off
  case PathList("javax", "servlet", xs@_*)         => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "log4j.propreties"                          => MergeStrategy.first
  // ----
  // required for spark-sql to read different data types (e.g. parquet/orc/csv...)
  // ----
  case PathList("META-INF", "services", xs@_*)     => MergeStrategy.first
  case PathList("META-INF", xs@_*)                 => MergeStrategy.discard
  case n if n.startsWith("reference.conf")         => MergeStrategy.concat
  case n if n.endsWith(".conf")                    => MergeStrategy.concat
  case x => MergeStrategy.first
  //@formatter:on
}

mainClass in assembly := Some("me.rotemfo.SparkApp")

test in assembly := {}