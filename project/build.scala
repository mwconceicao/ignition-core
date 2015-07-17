import sbt._
import Keys._

object IgnitionBuild extends Build {

  lazy val root = project.in(file("."))
    .dependsOn(file("chaordic"))
    .aggregate(file("chaordic"))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(
      name := "ignition-template",
      version := "1.0",
      scalaVersion := "2.10.4",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings"),
      // Because we can't run two spark contexts on same VM
      parallelExecution in Test := false,
      resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      libraryDependencies ++= Seq(
        ("org.apache.spark" %% "spark-core" % "1.3.0" % "provided").exclude("org.apache.hadoop", "hadoop-client"),
        "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.7.1" % "provided",
        "com.timgroup" %  "java-statsd-client"  % "3.0.2",
        "ch.qos.logback" % "logback-classic" % "1.0.13",
        ("com.sksamuel.elastic4s" %% "elastic4s" % "1.2.1.3").exclude("commons-io", "commons-io"),
        "org.scalaz" %% "scalaz-core" % "7.0.6",
        "org.scalatest" % "scalatest_2.10" % "2.0" % "test"))

}
