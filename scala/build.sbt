import sbt._

name := "spark-class-java"

version := "0.0.1-SNAPSHOT"

organization := "spark-class"

scalaVersion := "2.11.6"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies ++= Seq(
  "com.storm-enroute" % "scalameter_2.11" % "0.8.2",
  "org.apache.commons" % "commons-csv" % "1.1"
)

resolvers ++= Seq(
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

javaOptions in run += "-Xmx8G"

scalacOptions ++= Seq("-deprecation", "-unchecked")

//test in assembly := {}
