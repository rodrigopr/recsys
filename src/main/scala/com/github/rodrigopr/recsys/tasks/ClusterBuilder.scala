package com.github.rodrigopr.recsys.tasks

import java.util
import scala.collection.JavaConversions._

import com.typesafe.config.Config
import weka.clusterers.{Clusterer, SimpleKMeans, ClusterEvaluation}
import weka.core.{SparseInstance, Attribute, Instances}
import com.github.rodrigopr.recsys.{DataStore, StatsHolder, Task}
import com.github.rodrigopr.recsys.clusters.{FullItemFeature, DemographicFeature, GenreFeature, ClusterFeature}

object ClusterBuilder extends Task {
  private var attributesMap: Map[String, Attribute] = _
  private var userFeatures: Map[String, Map[String, Double]] = _

  def execute(config: Config) = {
    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(config.getInt("parallelism"))
    val numClusters = config.getInt("num-clusters")

    val features: Map[String, ClusterFeature] = Map(
      "genre" -> GenreFeature,
      "demographic" -> DemographicFeature,
      "full-item" -> FullItemFeature
    )

    val featuresConfigs = config.getConfig("features")
    val usedFeatures = featuresConfigs.root().entrySet().filter(f => features.containsKey(f.getKey)).map { entry =>
      val feature = entry.getKey
      val config = featuresConfigs.getConfig(feature).withFallback(featuresConfigs)
      features.get(feature).map(_.withConfig(config)).getOrElse(null)
    }.toList

    val attributes = usedFeatures.map(_.getFeatureList).flatten
    val totalAttributes = attributes.size

    val ids = 0.to(totalAttributes).iterator

    attributesMap = attributes.map { attribute =>
      val weighedAttribute = new Attribute(attribute._1, ids.next())
      weighedAttribute.setWeight(attribute._2)
      attribute._1 -> weighedAttribute
    }.toMap

    val dataset: Instances = generateDataset(usedFeatures)

    val cluster = executeCluster(dataset, numClusters, config)

    assignClusters(cluster, dataset)

    DataStore.groupClusterData()

    true
  }

  def assignClusters(cluster: Clusterer, dataset: Instances) {
      // for each user create a relationship with his calculated cluster
      userFeatures.foreach {
        case (userId, featureData) =>
          val instance = getInstance(featureData)
          instance.setDataset(dataset)

          // calculate the best cluster for the item
          val clusterNum = StatsHolder.timeIt("Cluster-Assign-Cluster") { cluster.clusterInstance(instance) }

          DataStore.setCluster(userId, clusterNum)
      }
  }

  def generateDataset(usedFeatures: Seq[ClusterFeature]): Instances = {
    // create the dataset with the feature list pre calculated
    val dataset = new Instances("data", new util.ArrayList[Attribute](attributesMap.values), 0)

    val usersIds = DataStore.users.keySet

    // in parallel calculate the feature data for each list
    userFeatures = usersIds.par.map{ user =>
      // get data from each feature configured, then reduce to a single map
      user -> usedFeatures.map(_.extractFeatures(user)).reduce(_ ++ _)
    }.seq.toMap

    val instances = userFeatures.values.map(getInstance)

    // add each instance to the dataset
    instances.seq.foreach(dataset.add)
    dataset
  }

  /**
   * transform the feature map in a WEKA's SparseInstance
   */
  def getInstance(featureMap: Map[String, Double]): SparseInstance = {
    val instance = new SparseInstance(attributesMap.size)
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
    StatsHolder.timeIt("Create-Cluster") {
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
}
