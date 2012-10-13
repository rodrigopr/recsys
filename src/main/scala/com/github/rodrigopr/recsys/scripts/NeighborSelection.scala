package com.github.rodrigopr.recsys.scripts

import scala.collection.JavaConversions._
import org.neo4j.graphdb.Node
import com.github.rodrigopr.recsys.Relation
import java.util.concurrent.atomic.AtomicLong
import scala.math._

class NeighborSelection extends BaseGraphScript {
  val useCluster = if(args.isEmpty)  false else args(0).toBoolean
  val numNeighbors = if(args.length > 1)  5 else args(1).toInt
  var totalTime = new AtomicLong
  var count = new AtomicLong

  collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(10)

  def timed(func: => Unit ) {
    val initialTime = System.currentTimeMillis
    func
    val computationTime = System.currentTimeMillis - initialTime
    Console.println("Finished one worker in " + computationTime + "ms")
    count.incrementAndGet()
    totalTime.addAndGet(computationTime)
  }

  indexUser.query("userid", "*").iterator().toTraversable.par.foreach(user =>  timed {
    val candidates = getNeighborsCandidates(user)
    val bestNeighbors = getBestNeighbors(candidates, numNeighbors)

    bestNeighbors.foreach{ neighbor =>
      val rel = user.createRelationshipTo(graphDB.getNodeById(neighbor._1), Relation.Neighbor)
      rel.setProperty("similarity", neighbor._2)

      val indexToAdd = if (useCluster) indexNeighborWithCluster else indexNeighbor
      indexToAdd.add(rel, "userId", user.getId)
    }
  })

  Console.out.println("Total time to process " + count.get + " users: " + totalTime.get() + "ms (media " + totalTime.get / count.get() + ")")

  def getBestNeighbors(mapUserRatings: List[Map[String, Any]], numNeighbors: Int): List[(Long, Double)] = {
    // Group candidates by id
    val candidateGroup = mapUserRatings.groupBy(map => map.get("candidateId").asInstanceOf[Long])

    def toRatingList(map: List[Map[String, Any]]) = map.map(item => Pair(item.get("rating1").asInstanceOf[Float], item.get("rating2").asInstanceOf[Float]))

    // Crate a list with pairs ID, Similarity
    val candidatesSim = candidateGroup.map(pair => Pair(pair._1, pearsonSimilarity(toRatingList(pair._2))))(collection.breakOut)

    // Sort list by user similarity decreasingly
    candidatesSim.sortBy(pair => pair._2 * -1)

    // return only the first N candidates
    candidatesSim.take(numNeighbors).toList
  }

  def pearsonSimilarity(ratingsInCommon: List[(Float, Float)]): Double = {
    if(ratingsInCommon.isEmpty) {
      return 0
    }

    var user1Sum = 0.0d
    var user2Sum = 0.0d
    var user1SumSquare = 0.0d
    var user2SumSquare = 0.0d
    var sumSquare = 0.0d

    ratingsInCommon.foreach{ ratingPair =>

      // Sum all common rates
      user1Sum = user1Sum + ratingPair._1
      user2Sum = user2Sum + ratingPair._2

      // Sum the squares
      user1SumSquare = user1SumSquare + pow(ratingPair._1, 2.0)
      user2SumSquare = user2SumSquare + pow(ratingPair._2, 2.0)

      // Sum the products
      sumSquare = sumSquare + (ratingPair._1 * ratingPair._2)
    }

    // Num of ratings in common
    val countRatingsInCommon = ratingsInCommon.size

    // Calculate Pearson Correlation score
    val numerator = sumSquare - ((user1Sum * user2Sum) / countRatingsInCommon)
    val deliminator = sqrt( (user1SumSquare - (pow(user1Sum,2) / countRatingsInCommon)) * (user2SumSquare - (pow( user2Sum,2) / countRatingsInCommon)))

    if(deliminator == 0)
      0
    else
      numerator / deliminator
  }

  def getNeighborsCandidates(user: Node): List[Map[String, Any]] = {
    var query = "START me = Node(" + user.getId + ") "

    if (useCluster) {
      query += ", cluster = node:inCluster('nodeId:" + user.getId + "') "
      query += "MATCH cluster<--candidate, me-[r :Rated]->movie<-[r2 :Rated]-candidate "
    } else {
      query += "MATCH me-[r :Rated]->movie<-[r2 :Rated]-candidate "
    }

    query += "RETURN return candidate.id as candidateId, r.rating as rating1, r2.rating as rating2;"

    engine.execute(query).toList
  }
}
