package com.github.rodrigopr.recsys.scripts

import java.util
import io.Source
import collection.mutable
import scala.collection.JavaConversions._
import scala.math._

import util.concurrent.ConcurrentHashMap
import weka.clusterers.{SimpleKMeans, ClusterEvaluation}
import weka.core.{SparseInstance, Attribute, Instances}
import com.redis.RedisClient.MAX
import com.github.rodrigopr.recsys.utils.Memoize

object ClusterBuilder extends BaseGraphScript {
  val users: mutable.ConcurrentMap[Long, mutable.Map[String, Double]] = new ConcurrentHashMap[Long, mutable.Map[String, Double]]
  val attributes = mutable.HashMap[String, Attribute]()

  var totalAttributes = 0
  val allGenres = Source.fromFile("resources/genre.dat").getLines().toList

  val movieGenreMemoized = Memoize.memoize((movieId: String) => {
    pool.withClient{ client =>
      client.smembers[String](buildKey("movie", movieId, "genres")).getOrElse(Set[Option[String]]()).map(_.get)
    }
  })

  def loadAttributes(): List[Attribute] = {
    allGenres.map { line =>
      val genreName = line
      attributes.put(genreName, new Attribute(genreName, totalAttributes))
      totalAttributes += 1

      attributes.get(genreName).get
    }.toList
  }

  implicit def iterableWithAvg[T:Numeric](data:Iterable[T]) = new {
    def avg = average(data)

    def average( ts: Iterable[T] )(implicit num: Numeric[T] ) = {
      num.toDouble( ts.sum ) / ts.size
    }
  }

  val instances = new Instances("data", new util.ArrayList[Attribute](loadAttributes()), 80000)

  collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(200)

  var usersIds = pool.withClient(client => client.smembers("users")).get.map(_.get)

  instances.addAll(
    seqAsJavaList(
      usersIds.par.map(user => { getInstance(getMap(user)) }).toList
    )
  )

  def getInstance(item: mutable.Map[String, Double]): SparseInstance = {
    val instance = new SparseInstance(totalAttributes)
    item.foreach(entry => {
      instance.setValue(attributes.get(entry._1).get, entry._2)
    })
    instance
  }

  def getMap(user: String, calcMaxAndMin: Boolean = true): mutable.HashMap[String, Double] = {
    Console.println("Calculating item: " + user)

    try {
      val ratingsSimple = pool.withClient( client => client.zrangeWithScore(buildKey("ratings", "user", user), 0)).get

       val ratings = ratingsSimple.map( pair => ( pair._1, pair._2, movieGenreMemoized(pair._1) ))

      def processGenre[T](fn: String => T) = allGenres.map(genre => Pair(genre, fn(genre)))

      val countGenres = processGenre(genre => ratings.count(_._3.contains(genre))).toMap
      val avgGenres = processGenre(genre => ratings.filter(_._3.contains(genre)).map(_._2).avg).toMap

      val interestMap = mutable.HashMap[String, Double]()

      var max = 0.0
      var min = 1.0

      allGenres.foreach(genre => {
        val totalCategory = countGenres.getOrElse(genre, 0)
        val avgRatingCat = avgGenres.getOrElse(genre, 0.0d)

        // Get the interest coeficient for the genre
        val likeFactor = log(1 + totalCategory) * pow(avgRatingCat, 2)
        interestMap.put(genre, likeFactor)

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
        })
      }

      users.put(user.toLong, interestMap)
      interestMap
    } catch {
      case ex: Exception => {
        Console.println("Error on item:" + user)
        throw ex
      }
    }
  }

  val numClusters = 100

  val cluster = new SimpleKMeans()
  cluster.setNumClusters(numClusters)
  cluster.setInitializeUsingKMeansPlusPlusMethod(true)
  cluster.buildClusterer(instances)

  val evaluator = new ClusterEvaluation()
  evaluator.setClusterer(cluster)
  evaluator.evaluateClusterer(instances)
  Console.println(evaluator.clusterResultsToString())

  // for each user create a relationship with his calculated cluster
  users.foreach { entry =>
    val instance = getInstance(entry._2)
    instance.setDataset(instances)

    val clusterNum = cluster.clusterInstance(instance)

    pool.withClient { client =>
      client.zadd(buildKey("cluster", clusterNum.toString), 0.0d, entry._1.toString)
      client.set(buildKey("user", entry._1.toString, "cluster"), clusterNum.toString)
    }
  }

  // group ratings per cluster
  pool.withClient(_.smembers("movies")).get.foreach { movieId =>
    0.to(numClusters-1).foreach { clusterNum =>
      val keyDest = buildKey("ratings", "movie", movieId.get, "cluster", clusterNum.toString)
      val keyRatingsMovie = buildKey("ratings", "movie", movieId.get)
      val keyCluster = buildKey("cluster", clusterNum.toString)
      pool.withClient(_.zinterstore(keyDest, List(keyRatingsMovie, keyCluster), MAX))
    }
  }
}
