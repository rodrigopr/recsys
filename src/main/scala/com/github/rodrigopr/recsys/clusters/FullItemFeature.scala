package com.github.rodrigopr.recsys.clusters

import com.typesafe.config.Config
import com.github.rodrigopr.recsys.DataStore

object FullItemFeature extends ClusterFeature {
  var weight: Double = 1.0

  def withConfig(config: Config): ClusterFeature = {
    weight = Option(config.getDouble("weight")).getOrElse(weight)
    return this
  }

  def getFeatureList = DataStore.movies.keySet.map(a => (a, weight)).toSeq

  def extractFeatures(userId: String): Map[String, Double] = DataStore.userRatings(userId)
}
