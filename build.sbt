// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sys.process._;

val localMavenHttps = "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-mavenrepo/"

resolvers ++= Seq(
  "Conjars" at "http://conjars.org/repo",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "S3 local maven snapshots" at localMavenHttps + "snapshots"
)

name := "telemetry-streaming"

version := "0.1-SNAPSHOT"

organization := "com.mozilla"

scalaVersion in ThisBuild := "2.11.8"

val sparkVersion = "2.2.0"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies += "com.mozilla.telemetry" %% "moztelemetry" % "1.0-SNAPSHOT",
    libraryDependencies += "com.mozilla.telemetry" %% "spark-hyperloglog" % "2.0.0-SNAPSHOT",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    libraryDependencies += "org.rogach" %% "scallop" % "1.0.2",
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0",
    libraryDependencies += "joda-time" % "joda-time" % "2.9.2",
    libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.0.1",
    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0",
    libraryDependencies += "com.github.tomakehurst" % "wiremock-standalone" % "2.14.0" % "provided",
    libraryDependencies += "com.github.java-json-tools" % "json-schema-validator" % "2.2.8"
  )

// Setup docker task
enablePlugins(DockerComposePlugin, DockerPlugin)
dockerImageCreationTask := docker.value
composeFile := sys.props.getOrElse("DOCKER_DIR", default = "docker/") + "docker-compose.yml"
variablesForSubstitutionTask := {
    val dockerKafkaHost: String = "./docker_setup.sh" !!;
    Map("DOCKER_KAFKA_HOST" -> dockerKafkaHost)
}

// Only run docker tasks on `sbt dockerComposeTest`
testOptions in Test += Tests.Argument("-l", "DockerComposeTag")

dockerfile in docker := {
  new Dockerfile {
    from("java")
  }
}

// make run command include the provided dependencies
run in Compile := { Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)) }

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

addCommandAlias("ci", ";clean ;compile ;scalastyle ;test:scalastyle ;coverage ;dockerComposeTest ;coverageReport")
