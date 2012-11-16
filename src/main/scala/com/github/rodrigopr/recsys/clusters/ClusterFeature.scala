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
