import com.typesafe.startscript.StartScriptPlugin

name := "graph_importer"

version := "1.0"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "org.neo4j" % "neo4j-lucene-index" % "1.9-SNAPSHOT" exclude("org.neo4j", "neo4j-udc"),
  "org.neo4j" % "neo4j-rest-graphdb" % "1.8-SNAPSHOT" exclude("org.neo4j", "neo4j-udc"),
  "org.neo4j" % "neo4j-cypher" % "1.9-SNAPSHOT" exclude("org.neo4j", "neo4j-udc"),

  "com.sun.jersey" % "jersey-core" % "1.9",
  "nz.ac.waikato.cms.weka" % "weka-dev" % "3.7.7"
)

/** Repos for Neo4j Admin server dep */
resolvers ++= Seq(
  "tinkerprop" at "http://tinkerpop.com/maven2",
  "neo4j-public-repository" at "http://m2.neo4j.org/releases",
  "neo4j-public-repository" at "http://m2.neo4j.org/snapshots",
  "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS releases" at "http://oss.sonatype.org/content/repositories/releases",
  "ScalaNLP" at "http://repo.scalanlp.org/repo"
)

seq(StartScriptPlugin.startScriptForClassesSettings: _*)
