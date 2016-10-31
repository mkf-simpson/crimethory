name := "crimethory"

version := "0.0.17"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "io.hydrosphere" %% "mist" % "0.6.0",
    "org.apache.spark" %% "spark-core" % "2.0.0",
    "org.apache.spark" %% "spark-sql" % "2.0.0",
    "org.apache.spark" %% "spark-hive" % "2.0.0",
    "org.apache.spark" %% "spark-mllib" % "2.0.0"
)