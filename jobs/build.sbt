name := "crimethory"

version := "0.0.9"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
    "io.hydrosphere" %% "mist-oldspark" % "0.5.0",
    "org.apache.spark" %% "spark-core" % "1.6.2",
    "org.apache.spark" %% "spark-sql" % "1.6.2",
    "org.apache.spark" %% "spark-hive" % "1.6.2"
)