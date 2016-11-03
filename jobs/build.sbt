name := "crimethory"

version := "0.0.18"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "io.hydrosphere" %% "mist" % "0.6.2",
    "org.apache.spark" %% "spark-core" % "2.0.0",
    "org.apache.spark" %% "spark-sql" % "2.0.0",
    "org.apache.spark" %% "spark-hive" % "2.0.0",
    "org.apache.spark" %% "spark-mllib" % "2.0.0",
    "org.apache.spark" %% "spark-streaming" % "2.0.0",
    "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0",
    "org.twitter4j" % "twitter4j-stream" % "4.0.4",
    "io.circe" %% "circe-core" % "0.5.1",
    "io.circe" %% "circe-generic" % "0.5.1",
    "io.circe" %% "circe-parser" % "0.5.1"
)
