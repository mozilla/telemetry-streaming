resolvers ++= Seq(
  "Conjars" at "http://conjars.org/repo",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

name := "telemetry-streaming"

version := "0.1-SNAPSHOT"

organization := "com.mozilla"

scalaVersion in ThisBuild := "2.11.8"

val sparkVersion = "2.1.0"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies += "com.mozilla.telemetry" %% "moztelemetry" % "1.0-SNAPSHOT",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    libraryDependencies += "org.rogach" %% "scallop" % "1.0.2",
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0",
    libraryDependencies += "joda-time" % "joda-time" % "2.9.2"
  )

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).inputTaskValue

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Don't run tests when assemblying the fat jar.
test in assembly := {}

// Default SBT settings suck
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Settings for RPM package generation
enablePlugins(JavaServerAppPackaging, RpmPlugin)
rpmRelease := "1"
rpmVendor := "mozilla"
version in Rpm := version.value.stripSuffix("-SNAPSHOT")
rpmUrl := Some("https://github.com/mozilla/telemetry-streaming")
rpmLicense := Some("MPLv2")
// autoServiceStart in Rpm := false

// removes all jar mappings in universal and appends the fat jar
mappings in Universal := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in Compile).value
  // removing means filtering
  val filtered = universalMappings filter {
    case (file, name) =>  ! name.endsWith(".jar")
  }
  // add the fat jar
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

// the bash script classpath only needs the fat jar
scriptClasspath := Seq((assemblyJarName in assembly).value)
