/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import scala.sys.process._;

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

val sparkVersion = "2.4.0"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies += "com.mozilla.telemetry" %% "moztelemetry" % "1.1-SNAPSHOT"
      exclude("org.json4s", "json4s-jackson_2.11"),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
      exclude("net.jpountz.lz4", "lz4"), //conflicts with org.lz4:lz4-java:1.4.0 from spark-core
    libraryDependencies += "org.eclipse.jetty" % "jetty-servlet" % "9.3.20.v20170531" % Provided, // needed for metrics
    libraryDependencies += "org.rogach" %% "scallop" % "1.0.2",
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.0.1" % Test,
    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0",
    libraryDependencies += "com.github.tomakehurst" % "wiremock-standalone" % "2.14.0" % "provided",
    libraryDependencies += "com.github.java-json-tools" % "json-schema-validator" % "2.2.8",
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0" % Test,
    libraryDependencies += "io.findify" %% "s3mock" % "0.2.5" % Test,
    libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.+" % "provided",
    libraryDependencies += "com.lihaoyi" %% "pprint" % "0.5.3"
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

// Add configs to resources
unmanagedResourceDirectories in Compile += baseDirectory.value / "configs"

parallelExecution in Test := false

scalacOptions ++= Seq(
  "-feature",
  "-Ywarn-unused",
  "-Ywarn-unused-import",
  "-Xfatal-warnings"
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// Shade PB classes - required for running on Databricks
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll,
  ShadeRule.rename("com.trueaccord.scalapb.**" -> "shadescalapb.@1").inAll
)

// Shorthand for locally running the full set of tests that would run in continuous integration.
addCommandAlias("ci", ";clean ;compile ;test:compile ;scalastyle ;test:scalastyle ;dockerComposeTest")

val scalaStyleConfigUrl = Some(url("https://raw.githubusercontent.com/mozilla/moztelemetry/master/scalastyle-config.xml"))
(scalastyleConfigUrl in Compile) := scalaStyleConfigUrl
(scalastyleConfigUrl in Test) := scalaStyleConfigUrl
