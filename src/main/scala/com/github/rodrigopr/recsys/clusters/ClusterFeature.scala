package com.github.rodrigopr.recsys.clusters

import collection.mutable
import com.github.rodrigopr.recsys.utils.RedisUtil._
import com.github.rodrigopr.recsys.datasets.{Movie, Genre}
import com.typesafe.config.Config

trait ClusterFeature {
  def withConfig(config: Config): ClusterFeature
  def getFeatureList: Seq[(String, Double)]
  def extractFeatures(userId: String): Map[String, Double]
  def postProcess(featureData: mutable.Map[String, Double]) {}
}

object ClusterFeature {
  lazy val allGenres: Set[Genre] = pool.withClient(_.smembers("genres")).get.map(g => Genre(g.get, g.get))
  lazy val allMovies: Map[String, Movie] = pool.withClient{ client =>
    client.smembers("movies").get.map(_.get).map { id =>
      val name = client.get(buildKey("movie", id, "name")).get
      val year = client.get(buildKey("movie", id, "year")).get.toInt
      val genres = client.smembers(buildKey("movie", id, "genres")).get.map(_.get).toSeq

      id -> Movie(id, name, year, genres)
    }.toMap
  }
}
