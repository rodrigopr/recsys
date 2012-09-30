package com.github.rodrigopr.recsys.scripts

import java.util
import io.Source
import collection.mutable
import scala.collection.JavaConversions._
import scala.math._

import org.neo4j.graphdb.Node

import weka.clusterers.{SimpleKMeans, ClusterEvaluation}
import weka.core.{SparseInstance, Attribute, Instances}
import com.github.rodrigopr.recsys.Relation

object ClusterBuilder extends BaseGraphScript {
  val users = mutable.Map[Long, mutable.Map[String, Double]]()
  val attributes = mutable.HashMap[String, Attribute]()

  val maxValue = mutable.HashMap[String, Double]()
  val minValue = mutable.HashMap[String, Double]()

  var totalAttributes = 0

  def loadAttributes(): List[Attribute] = {
    Source.fromFile("resources/u.genre").getLines().map{ line =>
      val genreName = line.split("\\|")(0)
      attributes.put(genreName, new Attribute(genreName, totalAttributes))
      totalAttributes += 1

      attributes.get(genreName).get
    }.toList
  }

  val instances = new Instances("data", new util.ArrayList[Attribute](loadAttributes()), 80000)

  instances.addAll(
    seqAsJavaList(
      indexUser.query("userid", "*").iterator().map(getMap(_)).toList.map(getInstance)
    )
  )


  def getInstance(item: mutable.Map[String, Double]): SparseInstance = {
    val instance = new SparseInstance(totalAttributes)
    item.foreach(entry => {
      val value = ((entry._2) - minValue(entry._1)) / (maxValue(entry._1) - minValue(entry._1))
      instance.setValue(attributes.get(entry._1).get, value)
    })
    instance
  }

  def getMap(node: Node, calcMaxAndMin: Boolean = true): mutable.HashMap[String, Double] = {
    Console.println("Calculating item: " + node.getProperty("id"))

    val userResult = engine.execute(
      "START me = Node(" + node.getId + ") " +
        "MATCH me-[r :Rated]->movie-[:Genre]->genre " +
        "RETURN genre.genre as genreName, " +
        "count(genre) as totalCat, " +
        "avg(r.rating) as avgRatingCat, " +
        "sqrt(count(genre)) * (avg(r.rating) * avg(r.rating)) as likeFactor;")

    val interestMap = mutable.HashMap[String, Double]()

    var max = 0.0
    var min = 1.0

    userResult.foreach(item => {
      val genreName = item.get("genreName").get.asInstanceOf[String]
      val totalCategory = item.get("totalCat").get.asInstanceOf[Long]
      val avgRatingCat = item.get("avgRatingCat").get.asInstanceOf[Double]

      // Get the interest coeficient for the genre
      val likeFactor = log(1 + totalCategory) * pow(avgRatingCat, 2)
      interestMap.put(genreName, likeFactor)

      if (likeFactor > max) {
        max = likeFactor
      } else if(likeFactor < min) {
        min = likeFactor
      }
    })

    if (!interestMap.isEmpty) {
      interestMap.foreach(entry => {
        val value: Double = (entry._2 - min) / (max - min)

        interestMap.put(entry._1, value)

        if (calcMaxAndMin) {
          if(value < minValue.getOrElse(entry._1, 2.0)) {
            minValue.put(entry._1, value)
          }
          if(value > maxValue.getOrElse(entry._1, -1.0)) {
            maxValue.put(entry._1, value)
          }
        }
      })
    }

    users.put(node.getId, interestMap)
    interestMap
  }

  val cluster = new SimpleKMeans()
  cluster.setNumClusters(10)
  cluster.setInitializeUsingKMeansPlusPlusMethod(true)
  cluster.buildClusterer(instances)

  val evaluator = new ClusterEvaluation()
  evaluator.setClusterer(cluster)
  evaluator.evaluateClusterer(instances)
  Console.println(evaluator.clusterResultsToString())


  val clustersNode = mutable.Map[Int, Node]()

  // Create the clusters node
  1.to(cluster.numberOfClusters()).par.foreach(createClusterNode)

  // for each user create a relationship with his calculated cluster
  users.par.foreach { entry =>
    val instance = getInstance(entry._2)
    instance.setDataset(instances)

    val clusterNum = cluster.clusterInstance(instance)

    val clusterNode = clustersNode.getOrElse(clusterNum, { createClusterNode(clusterNum) })

    doTx { db=>
      val user = graphDB.getNodeById(entry._1)
      user.createRelationshipTo(clusterNode, Relation.InCluster)
    }
  }

  def getClusterNode(clusterNum: Int): Node = {
    Option(indexCluster.get("clusterNum", clusterNum).getSingle).getOrElse(createClusterNode(clusterNum))
  }

  def createClusterNode(clusterNum: Int): Node = {
    doTx { db =>
      val clusterNode = db.createNode
      clusterNode.setProperty("clusterNum", clusterNum)
      indexCluster.add(clusterNode, "clusterNum", clusterNum)
    }

    val clusterNode = indexCluster.get("clusterNum", clusterNum).getSingle
    clustersNode.put(clusterNum, clusterNode)

    clusterNode
  }
}
