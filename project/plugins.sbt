addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.1")

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.0")

libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.5.45"
