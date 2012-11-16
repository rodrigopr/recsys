package com.github.rodrigopr.recsys.clusters

import scala.collection.JavaConversions._
import com.typesafe.config.Config
import com.github.rodrigopr.recsys.DataStore

object DemographicFeature extends ClusterFeature {
  var weight: Double = 2.0
  var features = Seq("age", "occupation", "gender")

  def withConfig(config: Config): ClusterFeature = {
    weight = config.getDouble("weight")
    features = Option(config.getStringList("items")).map(_.toSeq).getOrElse(features)
    return this
  }

  def getFeatureList = features.map(f => (f, weight))

  def extractFeatures(userId: String) = {
    val user = DataStore.users(userId)
    val genderVal = user.gender(0) match { case 'M' => 1.0d; case _ => 0.0d }

    Map[String, Double](
      "age" ->  ((user.age - 1) / 55.0),
      // "occupation" -> user.occupation,
      "gender" -> genderVal
    ).filterKeys(features.contains)
  }
}
