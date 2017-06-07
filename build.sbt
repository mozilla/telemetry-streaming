// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
resolvers ++= Seq(
  "Conjars" at "http://conjars.org/repo",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

name := "telemetry-streaming"

version := "0.1-SNAPSHOT"

organization := "com.mozilla"

scalaVersion in ThisBuild := "2.11.8"

val sparkVersion = "2.1.1"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies += "com.mozilla.telemetry" %% "moztelemetry" % "1.0-SNAPSHOT",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    libraryDependencies += "org.rogach" %% "scallop" % "1.0.2",
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0",
    libraryDependencies += "joda-time" % "joda-time" % "2.9.2"
  )

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Don't run tests when assemblying the fat jar.
test in assembly := {}

// Default SBT settings suck
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

addCommandAlias("ci", ";clean ;compile ;scalastyle ;coverage ;test ;coverageReport")