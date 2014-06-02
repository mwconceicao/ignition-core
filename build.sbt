name := "Ignition-core"

version := "0.2"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")

scalacOptions += "-target:jvm-1.7"

//scalacOptions += "-print" // Just for debugging purposes


ideaExcludeFolders += ".idea"

ideaExcludeFolders += ".idea_modules"

// Because we can't run two spark contexts on same VM
parallelExecution in Test := false

libraryDependencies ++= Seq()

libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.0.0" % "provided")

libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % "2.2.0" % "provided")

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "0.8.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "0.3.14"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"

resolvers += Resolver.sonatypeRepo("public")
