import com.typesafe.startscript.StartScriptPlugin

name := "graph_importer"

version := "1.0"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "net.debasishg" % "redisclient_2.9.1" % "2.7",
  "com.typesafe" % "config" % "1.0.0",
  "com.sun.jersey" % "jersey-core" % "1.9",
  "nz.ac.waikato.cms.weka" % "weka-dev" % "3.7.7",
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.0",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.0",
  "edu.ucla.sspace" % "sspace" % "2.0.3"
)

/** Repos for Neo4j Admin server dep */
resolvers ++= Seq(
  "tinkerprop" at "http://tinkerpop.com/maven2",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "ScalaNLP" at "http://repo.scalanlp.org/repo"
)

seq(StartScriptPlugin.startScriptForClassesSettings: _*)
