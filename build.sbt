name := "SimilaritySearchByRDF"

version := "1.0"

scalaVersion := "2.10.4"


testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

fork in run := true
javaOptions in run ++= Seq(
  "-Xms30G", "-Xmx32G", "-XX:MaxPermSize=8G", "-XX:+UseConcMarkSweepGC")


scalacOptions ++=
  Seq("-unchecked", "-Xlint", "-deprecation", "-Yno-adapted-args", "-feature")

fork := true

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-contrib_2.10" % "2.3.15",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.akka" % "akka-testkit_2.10" % "2.3.15",
  "junit" % "junit" % "4.11",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test",
  "com.google.guava" % "guava" % "18.0",
  "org.scalanlp" %% "breeze" % "0.13.2"
)