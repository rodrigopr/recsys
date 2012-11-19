package com.github.rodrigopr.recsys.clusters

import math._
import com.typesafe.config.Config

import com.github.rodrigopr.recsys.utils.ListAvg._
import com.github.rodrigopr.recsys.DataStore

object GenreFeature extends ClusterFeature {
  var weight: Double = 1.0

  def withConfig(config: Config): ClusterFeature = {
      weight = config.getDouble("weight")
      return this
  }

  def processGenre[T](fn: String => T) = DataStore.genres.map(genre => Pair(genre.id, fn(genre.id)))

  def getFeatureList = DataStore.genres.map(a => (a.name, weight)).toSeq

  def extractFeatures(userId: String) = {
    Console.println("Calculating item: " + userId)

    val ratings = DataStore.userRatings.getOrElse(userId, Map()).map{ case(movieId, rating) =>
      (movieId, rating, DataStore.movies(movieId).genre)
    }

    // count all ratings the user made for each genre
    val countGenres = processGenre(genre => ratings.count(_._3.contains(genre))).toMap

    // get the average ratings the user made for each genre
    val avgGenres = processGenre(genre => ratings.filter(_._3.contains(genre)).map(_._2).avg).toMap

    val interestMap = DataStore.genres.map { genre =>
      val totalCategory = countGenres.getOrElse(genre.id, 0)
      val likeFactor = totalCategory match {
        case 0 => 0
        case _ => {
          val avgRatingCat = avgGenres.getOrElse(genre.id, 0.0d)

          // Get the interest coefficient for the genre
          log(1 + totalCategory) * pow(avgRatingCat, 2)
        }
      }
      genre.id -> likeFactor
    }.toMap

    if(!interestMap.isEmpty) {
      val min = interestMap.values.min
      val max = interestMap.values.max
      interestMap.map{ case(id, likelihood) => (id, (likelihood - min) / (max - min)) }
    } else {
      interestMap
    }
  }
}
