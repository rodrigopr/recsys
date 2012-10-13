package com.github.rodrigopr.recsys.scripts

import org.neo4j.rest.graphdb.RestGraphDatabase
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.graphdb.GraphDatabaseService

class BaseGraphScript extends App {
  System.setProperty("org.neo4j.rest.stream", "true")

  val graphDB = new RestGraphDatabase("http://localhost:7474/db/data/")
  val engine = new ExecutionEngine(graphDB)

  val indexMovie = graphDB.index().forNodes("movie")
  val indexGenre = graphDB.index().forNodes("genre")
  val indexUser = graphDB.index().forNodes("user")
  val indexCluster = graphDB.index().forNodes("cluster")
  val indexInCLuster = graphDB.index().forNodes("inCluster")
  val indexNeighbor = graphDB.index().forRelationships("neighbor")
  val indexNeighborWithCluster = graphDB.index().forRelationships("neighborWithCluster")

  registerShutdownHook(graphDB)

  def registerShutdownHook(service: GraphDatabaseService) {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        service.shutdown()
      }
    })
  }

  def doTx[T](f: GraphDatabaseService => T) : T = {
    // not used transaction cause it was causing problem with the rest client for neo4j
    //val tx = graphDB.beginTx()
    try {
      f(graphDB)
      //tx.success()
    } finally {
      //tx.finish()
    }
  }
}
