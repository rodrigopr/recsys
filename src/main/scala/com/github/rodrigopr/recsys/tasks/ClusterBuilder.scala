package com.github.rodrigopr.recsys.tasks

import java.util
import collection.mutable
import scala.collection.JavaConversions._

import com.typesafe.config.Config
import weka.clusterers.{Clusterer, SimpleKMeans, ClusterEvaluation}
import weka.core.{SparseInstance, Attribute, Instances}
import com.redis.RedisClient.MAX
import com.github.rodrigopr.recsys.Task
import com.github.rodrigopr.recsys.utils.RedisUtil._
import com.github.rodrigopr.recsys.clusters.{GenreFeature, ClusterFeature}

object ClusterBuilder extends Task {

  def execute(config: Config) = {
    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(config.getInt("parallelSize"))
    val numClusters = config.getInt("numClusters")

    val features: Map[String, ClusterFeature] = Map(
      "genre" -> GenreFeature
    )
    val usedFeatures = config.getStringList("features").map(features.getOrElse(_, null)).filter(_ != null)

    val attributesName = usedFeatures.map(_.getFeatureList).flatten
    val totalAttributes = attributesName.size

    val ids = 0.to(totalAttributes).iterator
    val attributesMap = attributesName.map(a => a -> new Attribute(a, ids.next())).toMap

    // create the dataset with the feature list pre calculated
    val dataset = new Instances("data", new util.ArrayList[Attribute](attributesMap.values), 0)

    // fetch all users from redis
    val usersIds = pool.withClient(client => client.smembers("users")).get.map(_.get)
    val userFeatures = mutable.Map[String, Map[String, Double]]()

    // in parallel calculate the feature data for each list
    val instances = usersIds.par.map( user => {
      // get ata from each feature configured, then reduce to a single map
      val allFeatures = usedFeatures.map(_.extractFeatures(user)).reduce(_ ++ _)

      // salve the feature map for future use
      userFeatures.put(user, allFeatures)

      getInstance(allFeatures, totalAttributes, attributesMap)
    })

    // add each instance to the dataset
    instances.foreach(dataset.add)

    val cluster = executeCluster(dataset, numClusters, config)

    // for each user create a relationship with his calculated cluster
    userFeatures.foreach { case (userId, featureData) =>
      val instance = getInstance(featureData, totalAttributes, attributesMap)
      instance.setDataset(dataset)

      // calculate the best cluster for the item
      val clusterNum = cluster.clusterInstance(instance)

      // save in redis
      pool.withClient ( _.pipeline { client =>
        client.zadd(buildKey("cluster", clusterNum.toString), 0.0d, userId)
        client.set(buildKey("user", userId, "cluster"), clusterNum.toString)
      })
    }

    // group ratings per cluster
    pool.withClient(_.smembers("movies")).get.par.foreach { movieId =>
      0.to(numClusters-1).foreach { clusterNum =>
        val keyDest = buildKey("ratings", "movie", movieId.get, "cluster", clusterNum.toString)
        val keyRatingsMovie = buildKey("ratings", "movie", movieId.get)
        val keyCluster = buildKey("cluster", clusterNum.toString)
        pool.withClient(_.zinterstore(keyDest, List(keyRatingsMovie, keyCluster), MAX))
      }
    }

    true
  }

  /**
   * transform the feature map in a weka's SparceInstance
   */
  def getInstance(featureMap: Map[String, Double], totalAttributes: Int, attributesMap: Map[String, Attribute]): SparseInstance = {
    val instance = new SparseInstance(totalAttributes)
    featureMap.foreach { case (name, rating) =>
      instance.setValue(attributesMap.get(name).get, rating)
    }
    instance
  }

  /**
   * generate the clusters for the given dataset
   * @return
   */
  def executeCluster(dataset: Instances, numClusters: Int, config: Config): Clusterer = {
    val cluster = new SimpleKMeans()
    cluster.setNumClusters(numClusters)
    cluster.setInitializeUsingKMeansPlusPlusMethod(true)
    cluster.buildClusterer(dataset)

    // print the cluster data
    val evaluator = new ClusterEvaluation()
    evaluator.setClusterer(cluster)
    evaluator.evaluateClusterer(dataset)
    Console.println(evaluator.clusterResultsToString())

    cluster
  }
}
