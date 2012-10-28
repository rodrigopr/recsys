package com.github.rodrigopr.recsys.clusters

import com.github.rodrigopr.recsys.utils.RedisUtil._
import scala.collection.JavaConversions._
import com.typesafe.config.Config

object DemographicFeature extends ClusterFeature {
  var weight: Double = 2.0
  var features = Seq("age", "occupation", "gender")

  def withConfig(config: Config): ClusterFeature = {
    weight = config.getDouble("weight")
    features = Option(config.getStringList("items")).map(_.toSeq).getOrElse(features)
    return this
  }

  def getFeatureList = features.map(f => (f, weight))

  def extractFeatures(user: String) = {
    pool.withClient { client =>
      val age = client.get(buildKey("user", user, "age")).get.toDouble
      val occupation = client.get(buildKey("user", user, "occupation")).get.toDouble
      val gender = client.get(buildKey("user", user, "gender")).get
      val genderVal = gender(0) match { case 'M' => 1.0d; case _ => 0.0d }

      Map(
        "age" ->  age,
        "occupation" -> occupation,
        "gender" -> genderVal
      ).filterKeys(features.contains)
    }
  }
}
