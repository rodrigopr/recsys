package com.github.rodrigopr.recsys.tasks

import com.github.rodrigopr.recsys.{DataStore, StatsHolder, Task}
import com.typesafe.config.Config
import io.Source
import scala.math._
import java.util.concurrent.atomic.AtomicLong
import com.github.rodrigopr.recsys.datasets.Rating

object Recommender extends Task {
  val notRecommended = new AtomicLong
  val totalRecommended = new AtomicLong
  var userCluster: Boolean = _

  def execute(config: Config) = {
    val datasetConfig = config.getConfig("importer")
    val datasetParser = DatasetImporter.getDatasetParser(datasetConfig)
    val ratingData = Option(config.getString("rating-data")).getOrElse("1")
    val fileName = datasetConfig.getString("resource-prefix") + "/r" + ratingData + ".test"
    val testRatings = Source.fromFile(fileName).getLines().map(datasetParser.parseRating).toStream
    userCluster = config.getBoolean("user-cluster")

    val itemBased = config.getBoolean("item-based")

    val uErrors =  StatsHolder.timeIt("(User Based) Total Time ") {
      testRatings.map(calcError(rmse, userBasedPrediction, "User")).filter(-1 !=).toList
    }
    val uError = sqrt(uErrors.sum / uErrors.size)

    StatsHolder.setCustomData("(User Based) Recommendations Made", totalRecommended.get.toString)
    StatsHolder.setCustomData("(User Based) Recommendations Not Made", notRecommended.get.toString)
    StatsHolder.setCustomData("(User Based) Error rating(MAE)", "%.5f".format(uError))

    totalRecommended.getAndSet(0l)
    notRecommended.getAndSet(0l)

    if(itemBased) {
      val mErrors =  StatsHolder.timeIt("(Item Based) Total Time ") {
        testRatings.map(calcError(rmse, itemBasedPrediction, "Item")).filter(-1 !=).toList
      }
      val mError = sqrt(mErrors.sum / mErrors.size)

      StatsHolder.setCustomData("(Item Based) Recommendations Made", totalRecommended.get.toString)
      StatsHolder.setCustomData("(Item Based) Recommendations Not Made", notRecommended.get.toString)
      StatsHolder.setCustomData("(Item Based) Error rating(MAE)", "%.5f".format(mError))

      totalRecommended.getAndSet(0l)
      notRecommended.getAndSet(0l)
    }

    true
  }

  def rmse(predicted: Double, rating: Double): Double = pow(rating - predicted, 2)

  def mae(predicted: Double, rating: Double): Double = abs(rating - predicted)

  def userBasedPrediction(rating: Rating): Option[Double] = {
    val neighbours = DataStore.userNeighbours(rating.userId).filter(n => n.similarity > 0).map(n => n.id -> n.similarity).toMap

    val userClusterNum = DataStore.userCluster(rating.userId)

    val allMoviesRating = if(userCluster) DataStore.movieRatingsClustered(userClusterNum) else DataStore.movieRatings

    val movieRatings = allMoviesRating.getOrElse(rating.movieId, Map[String, Double]())

    val ratingsOfNeighbours = movieRatings.filterKeys(neighbours.contains)

    val userAvgRating = DataStore.avgRatingUser.getOrElse(rating.userId, 0.0d)

    if (ratingsOfNeighbours.isEmpty) {
      Console.println("No common rating to predict: " + rating)
      notRecommended.incrementAndGet()
      None
    } else {
      totalRecommended.incrementAndGet()

      val predictedSim = ratingsOfNeighbours.collect{ case (n, v) =>
        neighbours(n) * (v - DataStore.avgRatingUser(n))
      }.reduce(_+_)

      val similaritySum = ratingsOfNeighbours.map{ case (n, v) => neighbours(n)}.reduce(_+_)
      val predicted = round(userAvgRating + (predictedSim / similaritySum))

      Some(if (predicted > 5) 5 else predicted)
    }
  }

  def itemBasedPrediction(rating: Rating): Option[Double] = {
    val similarItems = DataStore.movieNeighbours(rating.movieId).map(n => n.id -> n.similarity).toMap

    val ratingsOfSimilarItems = DataStore.userRatings(rating.userId).filterKeys(similarItems.contains)

    if (ratingsOfSimilarItems.size < 1) {
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

    predicted.map(p => errorFunc(rating.rating, p)).getOrElse(-1)
  }
}
