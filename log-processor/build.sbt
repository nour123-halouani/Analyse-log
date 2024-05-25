name := "LogProcessor"

version := "1.0"

val scalaVersion_ = "2.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2", 
  "org.apache.spark" %% "spark-streaming" % "3.1.2", 
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.2", 
  "org.apache.kafka" %% "kafka" % "3.1.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.json4s" %% "json4s-native" % "3.6.11"
)


scalaSource in Compile := baseDirectory.value / "src"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-encoding", "utf8",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps"
)
