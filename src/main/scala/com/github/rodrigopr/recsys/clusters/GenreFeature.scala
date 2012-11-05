package com.github.rodrigopr.recsys.clusters

import ClusterFeature._
import com.github.rodrigopr.recsys.utils.RedisUtil._
import com.github.rodrigopr.recsys.utils.ListAvg._
import com.github.rodrigopr.recsys.utils.Memoize
import math._
import com.typesafe.config.Config

object GenreFeature extends ClusterFeature {
  val getMovieGenreMemoized = Memoize.memoize((movieId: String) => {
    pool.withClient{ client =>
      client.smembers[String](buildKey("movie", movieId, "genres")).getOrElse(Set[Option[String]]()).map(_.get)
    }
  })

  var weight: Double = 1.0

  def withConfig(config: Config): ClusterFeature = {
      weight = config.getDouble("weight")
      return this
  }

  def processGenre[T](fn: String => T) = allGenres.map(genre => Pair(genre.id, fn(genre.id)))

  def getFeatureList = allGenres.map(a => (a.name, weight)).toSeq

  def extractFeatures(user: String) = extractFeaturesMemoized(user)

  val extractFeaturesMemoized = Memoize.memoize((user: String) => {
    Console.println("Calculating item: " + user)

    val ratings = pool.withClient(
      _.zrangeWithScore(buildKey("ratings", "user", user), 0)
    ).get.map{ case(movieId, rating) => (movieId, rating, getMovieGenreMemoized(movieId)) }

    // count all ratings the user made for each genre
    val countGenres = processGenre(genre => ratings.count(_._3.contains(genre))).toMap

    // get the average ratings the user made for each genre
    val avgGenres = processGenre(genre => ratings.filter(_._3.contains(genre)).map(_._2).avg).toMap

    val interestMap = allGenres.filter(g => countGenres(g.id) > 0).map { genre =>
      val totalCategory = countGenres.getOrElse(genre.id, 0)
      val avgRatingCat = avgGenres.getOrElse(genre.id, 0.0d)

      // Get the interest coefficient for the genre
      val likeFactor = log(1 + totalCategory) * pow(avgRatingCat, 2)
      genre.id -> likeFactor
    }.toMap

    if(!interestMap.isEmpty) {
      val min = interestMap.values.min
      val max = interestMap.values.max
      interestMap.map{ case(id, likelihood) => (id, (likelihood - min) / (max - min)) }
    } else {
      interestMap
    }
  })
}
