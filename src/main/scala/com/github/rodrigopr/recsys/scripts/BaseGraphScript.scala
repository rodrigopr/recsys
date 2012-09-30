package com.github.rodrigopr.recsys.scripts

import org.neo4j.rest.graphdb.RestGraphDatabase
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.graphdb.GraphDatabaseService

class BaseGraphScript extends App {
  System.setProperty("org.neo4j.rest.stream", "true")

  val graphDB = new RestGraphDatabase("http://tccvps:7474/db/data/")
  val engine = new ExecutionEngine(graphDB)

  val indexMovie = graphDB.index().forNodes("movie")
  val indexGenre = graphDB.index().forNodes("genre")
  val indexUser = graphDB.index().forNodes("user")
  val indexCluster = graphDB.index().forNodes("cluster")

  registerShutdownHook(graphDB)

  def registerShutdownHook(service: GraphDatabaseService) {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        service.shutdown()
      }
    })
  }

  def doTx(f: GraphDatabaseService => Unit) {
    //val tx = graphDB.beginTx()
    try {
      f(graphDB)
      //tx.success()
    } finally {
      //tx.finish()
    }
  }
}
