name := "crimethory"

version := "0.0.19"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "io.hydrosphere" %% "mist" % "0.6.3",
    "org.apache.spark" %% "spark-core" % "2.0.0",
    "org.apache.spark" %% "spark-sql" % "2.0.0",
    "org.apache.spark" %% "spark-hive" % "2.0.0",
    "org.apache.spark" %% "spark-mllib" % "2.0.0",
    "org.apache.spark" %% "spark-streaming" % "2.0.0",
    "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0",
    "org.jpmml" % "jpmml-sparkml" % "1.1.4",
    "org.jpmml" % "pmml-model" % "1.3.4",
    "org.twitter4j" % "twitter4j-stream" % "4.0.4"
)
