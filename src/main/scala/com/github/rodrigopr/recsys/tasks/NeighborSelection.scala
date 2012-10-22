package com.github.rodrigopr.recsys.tasks

import java.util.concurrent.atomic.AtomicLong
import scala.math._
import com.github.rodrigopr.recsys.utils.Memoize
import com.github.rodrigopr.recsys.utils.RedisUtil._
import com.github.rodrigopr.recsys.Task
import com.typesafe.config.Config

object NeighborSelection extends Task {
  private val totalTime = new AtomicLong
  private val processedCount = new AtomicLong
  private var useCluster: Boolean = _
  private var numNeighbors: Int = _

  def execute(config: Config) = {
    useCluster = Option(config getBoolean "user-cluster") getOrElse false
    numNeighbors = Option(config getInt "num-neighbours") getOrElse 20

    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(config.getInt("parallelism"))

    pool.withClient(_.smembers("users")).get.par.foreach(user =>  timed {
      val candidates = getNeighborsCandidates(user.get)
      val bestNeighbors = getBestNeighbors(candidates, numNeighbors)

      bestNeighbors.foreach{ neighbor =>
        pool.withClient(_.zadd(buildKey("neighbours", user.get), neighbor._2, neighbor._1.toString))
      }
    })

    Console.out.println("Total time to process " + processedCount.get + " users: " + totalTime.get() + "ms (media " + totalTime.get / processedCount.get() + ")")

    true
  }

  def movieRatingsMemoized = Memoize.memoize((movieId: String, cluster: String) => {
    pool.withClient { client =>
      val key =
        if (useCluster)
          buildKey("ratings", "movie", movieId, "cluster", cluster)
        else
          buildKey("ratings", "movie", movieId)

      client.zrangeWithScore(key).get
    }
  })

  def timed[T](func: => T ) {
    val initialTime = System.currentTimeMillis
    val res = func
    val computationTime = System.currentTimeMillis - initialTime
    Console.println("Finished one worker in " + computationTime + "ms")
    processedCount.incrementAndGet()
    totalTime.addAndGet(computationTime)
    res
  }

  def getBestNeighbors(mapUserRatings: List[(String, Double, Double)], numNeighbors: Int): List[(String, Double)] = {
    // Group candidates by id
    val candidateGroup = mapUserRatings.groupBy(_._1)

    // Crate a list with pairs ID, Similarity
    val candidatesSim = candidateGroup.map(pair => Pair(pair._1, pearsonSimilarity(pair._2)))(collection.breakOut)

    // Sort list by user similarity decreasingly
    // return only the first N candidates
    candidatesSim.sortBy(pair => pair._2 * -1).take(numNeighbors).toList
  }

  def pearsonSimilarity(ratingsInCommon: List[(String, Double, Double)]): Double = {
    if(ratingsInCommon.isEmpty) {
      return 0
    }

    var user1Sum = 0.0d
    var user2Sum = 0.0d
    var user1SumSquare = 0.0d
    var user2SumSquare = 0.0d
    var sumSquare = 0.0d

    ratingsInCommon.foreach{ case (_, myRating, otherRating) =>

      // Sum all common rates
      user1Sum = user1Sum + myRating
      user2Sum = user2Sum + otherRating

      // Sum the squares
      user1SumSquare = user1SumSquare + pow(myRating, 2.0)
      user2SumSquare = user2SumSquare + pow(otherRating, 2.0)

      // Sum the products
      sumSquare = sumSquare + (myRating * otherRating)
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

  def getNeighborsCandidates(user: String): List[(String, Double, Double)] = {
    val cluster = pool.withClient(_.get(buildKey("user", user, "cluster"))).get
    val myRatings = pool.withClient(_.zrangeWithScore(buildKey("ratings", "user", user), 0)).get

    myRatings.foldLeft(List[(String, Double, Double)]()) {
      case (total, (movieId, myRating)) => {
        total ::: movieRatingsMemoized(movieId, cluster).filter(item => !(item._1.equals(user))).map(item => (item._1, myRating, item._2))
      }
    }
  }
}
