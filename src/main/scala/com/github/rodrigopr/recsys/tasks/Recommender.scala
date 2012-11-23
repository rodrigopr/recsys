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

    userCluster = config.getBoolean("use-cluster")
    totalRecommended.getAndSet(0l)
    notRecommended.getAndSet(0l)

    val algorithm = config.getString("algorithm")
    val algorithmPrefix = "(" + algorithm + " Based)"

    val func: (Rating) => Option[(Double,Double)] = algorithm match {
      case "user" => userBasedPrediction
      case "item" => itemBasedPrediction
      case "mf" => matrixFactorizationPrediction
      case _ => itemBasedPrediction
    }

    val predictions = StatsHolder.timeIt(algorithmPrefix + " Total Time ") {
        testRatings.map(func).toList.flatten
    }

    val maeError = mae(predictions)
    val rmseError = rmse(predictions)
    val maeErrorRound = mae(predictions, roundD)
    val rmseErrorRound = rmse(predictions, roundD)

    StatsHolder.setCustomData(algorithmPrefix + " Recommendations Made", totalRecommended.get.toString)
    StatsHolder.setCustomData(algorithmPrefix + " Recommendations Not Made", notRecommended.get.toString)
    StatsHolder.setCustomData(algorithmPrefix + " Error rating(MAE)", "%.5f".format(maeError))
    StatsHolder.setCustomData(algorithmPrefix + " Error rating(RMSE)", "%.5f".format(rmseError))
    StatsHolder.setCustomData(algorithmPrefix + " Error rating round(MAE)", "%.5f".format(maeErrorRound))
    StatsHolder.setCustomData(algorithmPrefix + " Error rating round(RMSE)", "%.5f".format(rmseErrorRound))

    true
  }

  def roundD(value: Double): Double = round(value)

  def rmse(predictions: List[(Double, Double)], applyFunc: (Double) => Double = abs): Double = {
    sqrt( predictions.map{ case(r, p) => applyFunc(pow(r - p, 2)) }.reduce(_+_) / predictions.size )
  }

  def mae(predictions: List[(Double, Double)], applyFunc: (Double) => Double = abs): Double = {
    predictions.map{ case(r, p) => applyFunc(abs(r - p))}.reduce(_+_) / predictions.size
  }

  def userBasedPrediction(rating: Rating): Option[(Double, Double)] = {
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
      val predicted = userAvgRating + (predictedSim / similaritySum)

      Some((predicted, rating.rating))
    }
  }

  def itemBasedPrediction(rating: Rating): Option[(Double, Double)] = {
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

      val predicted = (predictedSim / similaritySum)

      Some((predicted, rating.rating))
    }
  }

  def matrixFactorizationPrediction(rating: Rating): Option[(Double, Double)] = {
    val aproximated = DataStore.approximatedMatrix.get(rating.userId.toInt, rating.movieId.toInt) match {
      case i if i < 1 => 1.0
      case i if i > 5 => 5.0
      case i => i
    }

    Some(aproximated, rating.rating)
  }
}
