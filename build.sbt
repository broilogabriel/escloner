name := "escloner"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

libraryDependencies += "org.mockito" % "mockito-core" % "1.10.19" % "test"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.5"

mainClass in (Compile, run) := Some("com.broilogabriel.Main")