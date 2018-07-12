name := "rdb-connector-mysql"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.3"

resolvers += "jitpack" at "https://jitpack.io"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Ywarn-dead-code",
  "-Xlint"
)

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  val slickV = "3.2.0"
  Seq(
    "com.github.emartech" %  "rdb-connector-common"  % "9c88231514",
    "com.typesafe.slick"  %% "slick"                 % slickV,
    "com.typesafe.slick"  %% "slick-hikaricp"        % slickV,
    "mysql"               %  "mysql-connector-java"  % "5.1.38",
    "org.scalatest"       %% "scalatest"             % scalaTestV   % Test,
    "com.typesafe.akka"   %% "akka-stream-testkit"   % "2.5.6"      % Test,
    "com.github.emartech" %  "rdb-connector-test"    % "6769c3b36b" % Test,
    "com.typesafe.akka"   %% "akka-http-spray-json"  % "10.0.7"     % Test,
    "org.mockito"         %  "mockito-core"          % "2.11.0"     % Test
  )
}

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
.configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )
