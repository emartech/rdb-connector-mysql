name := "rdb-connector-mysql"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.3"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  val slickV = "3.2.0"
  Seq(
    "com.github.emartech" %  "rdb-connector-common"  % "90dc584205",
    "com.typesafe.slick"  %% "slick"                 % slickV,
    "com.typesafe.slick"  %% "slick-hikaricp"        % slickV,
    "mysql"               %  "mysql-connector-java"  % "5.1.38",
    "org.scalatest"       %% "scalatest"             % scalaTestV  % "test",
    "com.typesafe.akka"   %% "akka-stream-testkit"   % "2.5.6"     % "test",
    "com.github.emartech" %  "rdb-connector-test"    % "93ba63e9ee" % "test"
  )
}

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
.configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )