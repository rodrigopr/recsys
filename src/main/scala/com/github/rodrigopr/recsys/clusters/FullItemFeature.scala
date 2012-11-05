package com.github.rodrigopr.recsys.clusters

import com.typesafe.config.Config
import com.github.rodrigopr.recsys.clusters.ClusterFeature._
import com.github.rodrigopr.recsys.utils.RedisUtil._

object FullItemFeature extends ClusterFeature {
  var weight: Double = 1.0

  def withConfig(config: Config): ClusterFeature = {
    weight = config.getDouble("weight")
    return this
  }

  def getFeatureList = allMovies.keySet.map(a => (a, weight)).toSeq

  def extractFeatures(userId: String): Map[String, Double] = {
    pool.withClient(_.zrangeWithScore(buildKey("ratings", "user", userId), 0 , -1)).get.toMap
  }
}
