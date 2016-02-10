import scala.util.matching.Regex

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
  "io.netty"                           % "netty-all"                        % "4.0.33.Final" force(),
  "com.typesafe"                         % "config"                         % "1.3.0",
"com.typesafe.akka"                  % "akka-actor_2.11"                  % "2.3.14")

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

assemblyJarName in assembly := "dt.jar"

// exclude all the meta stuff with this regular expressio
val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "log4j", xs @ _*) => MergeStrategy.discard
  case meta(_) => MergeStrategy.discard
  case other => (assemblyMergeStrategy in assembly).value(other)
}