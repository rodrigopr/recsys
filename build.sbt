import com.typesafe.startscript.StartScriptPlugin

name := "graph_importer"

version := "1.0"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "net.debasishg" % "redisclient_2.9.1" % "2.7",
  "com.typesafe" % "config" % "1.0.0",
  "com.sun.jersey" % "jersey-core" % "1.9",
  "nz.ac.waikato.cms.weka" % "weka-dev" % "3.7.7"
)

/** Repos for Neo4j Admin server dep */
resolvers ++= Seq(
  "tinkerprop" at "http://tinkerpop.com/maven2",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS releases" at "http://oss.sonatype.org/content/repositories/releases",
  "ScalaNLP" at "http://repo.scalanlp.org/repo"
)

seq(StartScriptPlugin.startScriptForClassesSettings: _*)
