package com.github.rodrigopr.recsys.clusters

import ClusterFeature._
import collection.mutable
import com.github.rodrigopr.recsys.utils.RedisUtil._
import math._
import com.github.rodrigopr.recsys.utils.Memoize

object GenreFeature extends ClusterFeature {
  val getMovieGenreMemoized = Memoize.memoize((movieId: String) => {
    pool.withClient{ client =>
      client.smembers[String](buildKey("movie", movieId, "genres")).getOrElse(Set[Option[String]]()).map(_.get)
    }
  })

  def processGenre[T](fn: String => T) = allGenres.map(genre => Pair(genre.id, fn(genre.id)))

  def getFeatureList = allGenres.map(_.name).toSeq

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

    val max = interestMap.values.max
    val min = interestMap.values.min

    interestMap.map{ case(id, likelihood) => (id, (likelihood - min) / (max - min)) }
  })
}
