name := """drivertest"""

version := "1.0"

scalaVersion := "2.11.7"

val driverCore = "3.0.0"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "ch.qos.logback"                     % "logback-classic"                  %  "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.datastax.cassandra"             % "cassandra-driver-core"            % driverCore,
  "com.typesafe.scala-logging"        %% "scala-logging"                    % "3.1.0",
  "com.typesafe"                         % "config"                         % "1.3.0")

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

