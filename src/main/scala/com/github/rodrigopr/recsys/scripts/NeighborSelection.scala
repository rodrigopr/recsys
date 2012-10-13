package com.github.rodrigopr.recsys.scripts

import collection.SortedSet
import scala.collection.JavaConversions._
import org.neo4j.graphdb.Node
import com.github.rodrigopr.recsys.Relation
import java.util.concurrent.atomic.AtomicLong

class NeighborSelection extends BaseGraphScript {
  val shouldUseCluster = if(args.isEmpty)  false else args(0).toBoolean
  val numNeighbors = if(args.length > 1)  5 else args(1).toInt
  var totalTime = new AtomicLong

  collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(10)

  def timed(func: => Unit ) {
    val initialTime = System.currentTimeMillis
    func
    val computationTime = System.currentTimeMillis - initialTime
    Console.println("Finished one worker in " + computationTime + "ms")
    totalTime.addAndGet(computationTime)
  }

  indexUser.query("userid", "*").iterator().toTraversable.par.foreach(user =>  timed {
    val candidates = getNeighborsCandidates(user)
    val bestNeighbors = getBestNeighbors(candidates, numNeighbors)

    bestNeighbors.foreach{ neighbor =>
      user.createRelationshipTo(graphDB.getNodeById(neighbor._2), Relation.Neighbor)
    }
    totalTime.addAndGet(System.currentTimeMillis())
  })

  def getBestNeighbors(mapUserRatings: List[Map[String, Any]], numNeighbors: Int): SortedSet[Pair[Double, Int]] = {
    throw new RuntimeException
  }

  def getNeighborsCandidates(user: Node): List[Map[String, Any]] = {
    var query = "START me = Node(" + user.getId + ") "
    if (shouldUseCluster) {
      query += ", cluster = node:inCluster('nodeId:" + user.getId + "') "
      query += "MATCH cluster<--outro, me-[r :Rated]->movie<-[r2 :Rated]-outro "
    } else {
      query += "MATCH me-[r :Rated]->movie<-[r2 :Rated]-outro "
    }
    query += "RETURN return outro.id, r.rating, r2.rating;"

    engine.execute(query).toList
  }
}
