package com.github.rodrigopr.recsys.tasks

import java.util.concurrent.atomic.AtomicLong
import scala.math._
import com.github.rodrigopr.recsys.utils.Memoize
import com.github.rodrigopr.recsys.utils.RedisUtil._
import com.github.rodrigopr.recsys.Task
import com.typesafe.config.Config

case class CommonRating(movieId: String, uRating: Double, oRating: Double)
case class Neighbour(id: String, similarity: Double, agreement: Int, diff: Double)

object NeighborSelection extends Task {
  private val totalTime = new AtomicLong
  private val processedCount = new AtomicLong
  private var useCluster: Boolean = _
  private var numNeighbors: Int = _

  def execute(config: Config) = {
    useCluster = Option(config getBoolean "user-cluster") getOrElse false
    numNeighbors = Option(config getInt "num-neighbours") getOrElse 20

    pool.withClient(_.smembers("users")).get.foreach( user =>  timed {
      val candidates = getUserNeighborCandidatesRatings(user.get)
      val bestNeighbors = getBestNeighbors(candidates, numNeighbors)

      bestNeighbors.foreach{ neighbor =>
        pool.withClient(_.zadd(buildKey("neighbours", user.get), neighbor.similarity, neighbor.id))
        pool.withClient(_.set(buildKey("neighbour", "diff", user.get, neighbor.id), neighbor.diff.toString))
      }
    })

    totalTime.set(0l)
    processedCount.set(0l)
    Console.println("Finish processing users")
    Console.println("Processing movies")

    pool.withClient(_.smembers("movies")).get.foreach( movie =>  timed {
      val candidates = getMovieNeighborCandidatesRatings(movie.get)
      val bestNeighbors = getBestNeighbors(candidates, numNeighbors)

      bestNeighbors.foreach{ neighbor =>
        pool.withClient(_.zadd(buildKey("similaritems", movie.get), neighbor.similarity, neighbor.id))
      }
    })

    Console.out.println("Total time to process " + processedCount.get + " users: " + totalTime.get() + "ms (media " + totalTime.get / processedCount.get() + ")")

    true
  }

  val movieRatingsMemoized = Memoize.memoize((movieId: String, cluster: String) => {
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
    Console.println("Finished one worker in " + computationTime + "ms - Total processed: " + processedCount.get)
    processedCount.incrementAndGet()
    totalTime.addAndGet(computationTime)
    res
  }

  def getBestNeighbors(mapUserRatings: List[CommonRating], numNeighbors: Int): List[Neighbour] = {
    // Group candidates by id
    val candidateGroup = mapUserRatings.groupBy(c => c.movieId)

    // Crate a list with pairs ID, Similarity
    val candidatesSim = candidateGroup.map { case (candidate, commonRatings) =>
      Neighbour(candidate, pearsonSimilarity(commonRatings), agreement(commonRatings), diff(commonRatings))
    }.toSeq

    // Sort list by user similarity decreasingly
    // return only the first N candidates
    candidatesSim.sortBy(c => (c.agreement, c.similarity)).takeRight(numNeighbors).toList
  }

  def diff(ratingsInCommon: List[CommonRating]): Double = {
    val otherVotes = ratingsInCommon.foldLeft(0.0d)((a, b) => a + b.oRating)
    if(otherVotes > 0) {
      ratingsInCommon.foldLeft(0.0d)((a, b) => a + b.uRating) / otherVotes
    } else {
      0
    }
  }

  def agreement(ratingsInCommon: List[CommonRating]): Int =  ratingsInCommon.count(r => r.oRating == r.uRating)

  def pearsonSimilarity(ratingsInCommon: List[CommonRating]): Double = {
    if(ratingsInCommon.isEmpty) {
      return 0
    }

    var user1Sum = 0.0d
    var user2Sum = 0.0d
    var user1SumSquare = 0.0d
    var user2SumSquare = 0.0d
    var sumSquare = 0.0d

    ratingsInCommon.foreach{ case CommonRating(_, myRating, otherRating) =>

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
    val denominator = sqrt( (user1SumSquare - (pow(user1Sum,2) / countRatingsInCommon)) * (user2SumSquare - (pow( user2Sum,2) / countRatingsInCommon)))


    denominator match {
      case 0 => 0
      case _ => (1 + numerator / denominator) / 2
    }
  }

  def getUserNeighborCandidatesRatings(user: String): List[CommonRating] = {
    pool.withClient(_.get(buildKey("user", user, "cluster"))).map { cluster =>
      val myRatings = pool.withClient(_.zrangeWithScore(buildKey("ratings", "user", user), 0)).get

      myRatings.foldLeft(List[CommonRating]()) {
        case (total, (movieId, myRating)) => {
          val ratingsFromOthersUsers = movieRatingsMemoized(movieId, cluster).filterNot(r => user.equals(r._1))

          total ::: ratingsFromOthersUsers.map{
            case(oUser, rating) => CommonRating(oUser, myRating, rating)
          }
        }
      }
    }.getOrElse(List())
  }

  def getMovieNeighborCandidatesRatings(movie: String): List[CommonRating] = {
    val genres = pool.withClient(_.smembers(buildKey("movie", movie, "genres"))).get.map(_.get)
    val usersThatRatedMe = pool.withClient(_.zrangeWithScore(buildKey("ratings", "movie", movie), 0)).get

    usersThatRatedMe.foldLeft(List[CommonRating]()) {
      case (total, (userId, myRating)) => {
        val keyBase: (String) => String = buildKey("ratings", "user", userId, "genre", _)

        val ratingsInCommon = genres.map { genre =>
          pool.withClient(_.zrangeWithScore(keyBase(genre))).get.filterNot(r => movie.equals(r._1))
        }
        if (ratingsInCommon.size > 0)
          total ::: ratingsInCommon.reduce(_ ::: _).map{ case(u, r) => CommonRating(u, myRating, r) }
        else
          total
      }
    }
  }
}
