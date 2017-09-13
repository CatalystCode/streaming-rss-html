organization := "com.github.catalystcode"
name := "streaming-rss-html"
description := "A library for reading public RSS feeds and public websites using Spark Streaming."

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature"
)

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
).map(_ % "compile")

libraryDependencies ++= Seq(
  "com.rometools" % "rome" % "1.8.0",
  "log4j" % "log4j" % "1.2.17"
)

libraryDependencies ++= Seq(
  "org.mockito" % "mockito-core" % "2.8.47",
  "org.mockito" % "mockito-inline" % "2.8.47",
  "org.scalatest" %% "scalatest" % "2.2.1"
).map(_ % "test")

assemblyMergeStrategy in assembly := {                                          
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard                    
 case x => MergeStrategy.first                                                  
}                                                                               

