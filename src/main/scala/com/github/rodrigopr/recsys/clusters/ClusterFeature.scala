package com.github.rodrigopr.recsys.clusters

import collection.mutable
import com.github.rodrigopr.recsys.utils.RedisUtil._
import com.github.rodrigopr.recsys.datasets.Genre
import scala.Numeric

trait ClusterFeature {
  def getFeatureList: Seq[String]
  def extractFeatures(userId: String): Map[String, Double]
  def postProcess(featureData: mutable.Map[String, Double]) {}
}

object ClusterFeature {
  class WeighedFeatureMap(featureData: Map[String, Double]) {
    def withWeigh(weigh: Double): Map[String, Double] = {
      null
    }
  }
  implicit def toWeighedFeatureMap(featureMap: Map[String, Double]) = new WeighedFeatureMap(featureMap)

  lazy val allGenres: Set[Genre] = pool.withClient(_.smembers("genres")).get.map(g => Genre(g.get, g.get))
  lazy val allMovies = pool.withClient{ client =>
    throw new RuntimeException //TODO: Implement
  }

  implicit def iterableWithAvg[T:Numeric](data:Iterable[T]) = new {
    def avg = average(data)

    def average( ts: Iterable[T] )(implicit num: Numeric[T] ) = {
      num.toDouble( ts.sum ) / ts.size
    }
  }
}
