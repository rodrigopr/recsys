package com.github.rodrigopr.recsys.tasks

import com.github.rodrigopr.recsys.Task
import com.typesafe.config.Config
import io.Source
import scala.math._
import java.util.concurrent.atomic.AtomicLong
import com.github.rodrigopr.recsys.datasets.Rating
import com.github.rodrigopr.recsys.utils.RedisUtil._

//TODO: Refactor this class!
object Recommender extends Task {
  val notRecommended = new AtomicLong
  val totalRecommended = new AtomicLong
  var userCluster: Boolean = _

  def execute(config: Config) = {
    val datasetConfig = config.getConfig("components.importer")
    val datasetParser = DatasetImporter.getDatasetParser(datasetConfig)
    val ratingData = Option(config.getString("rating-data")).getOrElse("1")
    val fileName = datasetConfig.getString("resource-prefix") + "/r" + ratingData + ".test"
    val testRatings = Source.fromFile(fileName).getLines().map(datasetParser.parseRating).toList
    userCluster = config.getBoolean("user-cluster")

    val uErrors = testRatings.map(calcUserBasedError(mae)).filter(_ != -1).toList

    val uError = sqrt(uErrors.sum / uErrors.size)

    val totalRecommencedUser = totalRecommended.getAndSet(0l)
    val totalNotRecommencedUser = notRecommended.getAndSet(0l)

    val mErrors = testRatings.map(calcMovieBasedError(mae)).filter(_ != -1).toList
    val mError = sqrt(mErrors.sum / mErrors.size)

    Console.println()
    Console.println()
    Console.println("==============================================")
    Console.println("User Based: ")
    Console.println("Recommendations made: " + totalRecommencedUser)
    Console.println("Recommendations not made: " + totalNotRecommencedUser)
    Console.println("Error rating(MAE): " + uError)

    Console.println()
    Console.println()
    Console.println("==============================================")
    Console.println("Item Based: ")
    Console.println("Recommendations made: " + totalRecommended.get)
    Console.println("Recommendations not made: " + notRecommended.get)
    Console.println("Error rating(MAE): " + mError)

    true
  }

  def mape(predicted: Double, rating: Double): Double = abs(rating - predicted) / rating

  def rmse(predicted: Double, rating: Double): Double = pow(rating - predicted, 2)

  def mae(predicted: Double, rating: Double): Double = abs(rating - predicted)

  def calcUserBasedError(errorFunc: (Double, Double) => Double = rmse)(rating: Rating): Double = {
    val neighboursKey = buildKey("neighbours", rating.userId)
    val neighbours = pool.withClient(_.zrangeWithScore(neighboursKey, 0, -1)).get.toMap.filter(_._2 > 0)

    val movieRatingKey = userCluster match  {
      case true => {
        val cluster = pool.withClient(_.get(buildKey("user", rating.userId, "cluster"))).getOrElse("0")
        buildKey("ratings", "movie", rating.movieId, "cluster", cluster)
      }
      case false => buildKey("ratings", "movie", rating.movieId)
    }

    val movieRatings = pool.withClient(_.zrangeWithScore(movieRatingKey, 0, -1)).get.toMap

    val ratingsOfNeighbours = movieRatings.filterKeys(neighbours.contains)

    val diffNeighbour = neighbours.map { case (oUserId, _) =>
      oUserId -> pool.withClient(_.get(buildKey("neighbour", "diff", rating.userId, oUserId))).get.toDouble
    }

    if (ratingsOfNeighbours.isEmpty) {
      Console.println("No common rating to predict: " + rating)
      notRecommended.incrementAndGet()
      -1
    } else {
      totalRecommended.incrementAndGet()
      val predictedSim = ratingsOfNeighbours.map{ case (n, v) =>
        neighbours(n) * v * diffNeighbour(n)
      }.reduce(_+_)
      val similaritySum = ratingsOfNeighbours.map{ case (n, v) => neighbours(n)}.reduce(_+_)


      var predicted = round((predictedSim / similaritySum))
      if (predicted > 5) {
        predicted = 5
      }

      Console.println("Predicted " + predicted + ", expected: " + rating.rating)
      errorFunc(rating.rating, predicted)
    }
  }

  def calcMovieBasedError(errorFunc: (Double, Double) => Double = rmse)(rating: Rating): Double = {
    val similarItemKey = buildKey("similaritems", rating.movieId)
    val similarItems = pool.withClient(_.zrangeWithScore(similarItemKey, 0, -1)).get.toMap.filter(_._2 > 0).toMap

    val myRatings = pool.withClient(_.zrangeWithScore(buildKey("ratings", "user", rating.userId))).get.toMap

    val ratingsOfSimilarItems = myRatings.filterKeys(similarItems.contains)

    if (ratingsOfSimilarItems.isEmpty) {
      Console.println("No common rating to predict: " + rating)
      notRecommended.incrementAndGet()
      -1
    } else {
      totalRecommended.incrementAndGet()
      val predictedSim = ratingsOfSimilarItems.map{ case (n, v) => similarItems(n) * v }.reduce(_+_)
      val similaritySum = ratingsOfSimilarItems.map{ case (n, v) => similarItems(n)}.reduce(_+_)


      var predicted = round((predictedSim / similaritySum))
      if (predicted > 5) {
        predicted = 5
      }

      Console.println("Predicted " + predicted + ", expected: " + rating.rating)
      errorFunc(rating.rating, predicted)
    }
  }
}
