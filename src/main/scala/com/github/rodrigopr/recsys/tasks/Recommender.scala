package com.github.rodrigopr.recsys.tasks

import com.github.rodrigopr.recsys.{StatsHolder, Task}
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
    val datasetConfig = config.getConfig("importer")
    val datasetParser = DatasetImporter.getDatasetParser(datasetConfig)
    val ratingData = Option(config.getString("rating-data")).getOrElse("1")
    val fileName = datasetConfig.getString("resource-prefix") + "/r" + ratingData + ".test"
    val testRatings = Source.fromFile(fileName).getLines().map(datasetParser.parseRating).toList
    userCluster = config.getBoolean("user-cluster")

    val uErrors = testRatings.map(calcError(mae, userBasedPrediction, "User")).filter(-1 !=).toList
    val uError = sqrt(uErrors.sum / uErrors.size)

    StatsHolder.setCustomData("(User Based) Recommendations Made", totalRecommended.get.toString)
    StatsHolder.setCustomData("(User Based) Recommendations Not Made", notRecommended.get.toString)
    StatsHolder.setCustomData("(User Based) Error rating(MAE)", "%.5f".format(uError))

    totalRecommended.getAndSet(0l)
    notRecommended.getAndSet(0l)

    val mErrors = testRatings.map(calcError(mae, itemBasedPrediction, "Item")).filter(-1 !=).toList
    val mError = sqrt(mErrors.sum / mErrors.size)

    StatsHolder.setCustomData("(Item Based) Recommendations Made", totalRecommended.get.toString)
    StatsHolder.setCustomData("(Item Based) Recommendations Not Made", notRecommended.get.toString)
    StatsHolder.setCustomData("(Item Based) Error rating(MAE)", "%.5f".format(mError))

    true
  }

  def rmse(predicted: Double, rating: Double): Double = pow(rating - predicted, 2)

  def mae(predicted: Double, rating: Double): Double = abs(rating - predicted)

  def userBasedPrediction(rating: Rating): Option[Double] = {
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

    //val diffNeighbour = neighbours.map { case (oUserId, _) =>
    //  oUserId -> pool.withClient(_.get(buildKey("neighbour", "diff", rating.userId, oUserId))).get.toDouble
    //}

    val avgUser = pool.withClient(_.get(buildKey("avgrating", rating.userId))).get.toDouble

    val avgNeighbour = neighbours.map { case (oUserId, _) =>
      oUserId -> pool.withClient(_.get(buildKey("avgrating", oUserId))).get.toDouble
    }

    if (ratingsOfNeighbours.isEmpty) {
      Console.println("No common rating to predict: " + rating)
      notRecommended.incrementAndGet()
      None
    } else {
      totalRecommended.incrementAndGet()
      val predictedSim = ratingsOfNeighbours.collect{ case (n, v) =>
        neighbours(n) * (v - avgNeighbour(n))
      }.reduce(_+_)
      val similaritySum = ratingsOfNeighbours.map{ case (n, v) => neighbours(n)}.reduce(_+_)

      val predicted = round(avgUser + (predictedSim / similaritySum))

      Some(if (predicted > 5) 5 else predicted)
    }
  }

  def itemBasedPrediction(rating: Rating): Option[Double] = {

    val similarItemKey = buildKey("similaritems", rating.movieId)
    val similarItems = pool.withClient(_.zrangeWithScore(similarItemKey, 0, -1)).get.toMap.filter(_._2 > 0).toMap

    val myRatings = pool.withClient(_.zrangeWithScore(buildKey("ratings", "user", rating.userId))).get.toMap

    val ratingsOfSimilarItems = myRatings.filterKeys(similarItems.contains)

    if (ratingsOfSimilarItems.isEmpty) {
      Console.println("No common rating to predict: " + rating)
      notRecommended.incrementAndGet()
      None
    } else {
      totalRecommended.incrementAndGet()
      val predictedSim = ratingsOfSimilarItems.map{ case (n, v) => similarItems(n) * v }.reduce(_+_)
      val similaritySum = ratingsOfSimilarItems.map{ case (n, v) => similarItems(n)}.reduce(_+_)

      val predicted = round((predictedSim / similaritySum))

      Some(if (predicted > 5) 5 else predicted)
    }
  }

  def calcError(errorFunc: (Double, Double) => Double, predictRating: Rating => Option[Double], name: String)(rating: Rating): Double = {
    val predicted = StatsHolder.timeIt("Predict-Rating-" + name, increment = true) { predictRating(rating) }

    predicted.map(p => StatsHolder.incr("Prediction-%s-%d-%d".format(name, round(p), round(rating.rating))))

    Console.println("Predicted " + predicted + ", expected: " + rating.rating)

    predicted.map(v => errorFunc(rating.rating, v)).getOrElse(-1)
  }
}
