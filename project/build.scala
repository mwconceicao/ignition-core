import sbt._
import sbt.Keys._

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
      libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.3.0" % "provided")
        .exclude("org.apache.hadoop", "hadoop-client"),
      libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.7.1" % "provided"),
      libraryDependencies += ("com.timgroup"        %  "java-statsd-client"  % "3.0.2"),
      libraryDependencies += "ch.qos.logback"      % "logback-classic" % "1.0.13",
      libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6",
      libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
  )
}
