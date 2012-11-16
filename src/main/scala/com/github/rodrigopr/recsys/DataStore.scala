package com.github.rodrigopr.recsys

import datasets._
import collection.mutable
import collection.immutable.HashMap
import datasets.Genre
import datasets.Movie
import datasets.Rating
import datasets.User
import utils.ListAvg._

object DataStore {
  val userRatings = mutable.HashMap[String, Map[String, Double]]()
  val movieRatings = mutable.HashMap[String, Map[String, Double]]()
  var movieRatingsClustered: Map[Long, Map[String, Map[String, Double]]] = _

  /**
   * Only work if called after all inserts happens
   */
  var avgRatingUser: Map[String, Double] = _

  val movies = mutable.HashMap[String, Movie]()
  val users = mutable.HashMap[String, User]()
  val genres = mutable.HashSet[Genre]()

  val clusters = mutable.HashMap[Long, List[String]]()
  val userCluster = mutable.HashMap[String, Long]()

  val userNeighbours = mutable.HashMap[String, List[Neighbour]]()
  val movieNeighbours = mutable.HashMap[String, List[Neighbour]]()

  def clear() {
    userRatings.clear()
    movieRatings.clear()
    movieRatingsClustered = null
    avgRatingUser = null
    movies.clear()
    users.clear()
    genres.clear()
    clusters.clear()
    userCluster.clear()
    userNeighbours.clear()
    movieNeighbours.clear()

    System.gc()
  }

  def registerRating(rating: Rating) {
    val userRatingMap = userRatings.getOrElse(rating.userId, HashMap[String, Double]())
    userRatings.update(rating.userId, userRatingMap.+( (rating.movieId, rating.rating)  ))

    val movieRatingMap = movieRatings.getOrElse(rating.movieId, HashMap[String, Double]())
    movieRatings.update(rating.movieId, movieRatingMap.+((rating.userId, rating.rating)))
  }

  def calcUserAvgRating() {
    avgRatingUser = userRatings.map{ case(user, ratings) =>
      user -> ratings.map(_._2).avg
    }.toMap
  }

  def setCluster(userId: String, cluster: Long) {
    val clusterUsers  = clusters.getOrElse(cluster, List[String]())
    clusters.update(cluster, userId :: clusterUsers)

    userCluster.put(userId, cluster)
  }

  def groupClusterData() {
    movieRatingsClustered = clusters.map{ case(clusterNum, clusterUsers) =>
      clusterNum -> (movieRatings.map{ case(item, ratings) => item -> (Map[String, Double]() ++ ratings.filterKeys(clusterUsers.contains)) }).toMap
    }.toMap
  }
}
