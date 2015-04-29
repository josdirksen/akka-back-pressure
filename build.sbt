name := "akka-back-pressure"

version := "1.0"

scalaVersion := "2.11.6"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-M5",
  "org.akkamon" %% "akka-mon" % "1.0-SNAPSHOT",
  "org.scalaj" %% "scalaj-http" % "1.1.4",
  "com.typesafe.play" %% "play-json" % "2.3.4"
)

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
    