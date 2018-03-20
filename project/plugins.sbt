addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.3")

addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.34")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.5.0")

addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.14.0")
