package com.github.rodrigopr.recsys.tasks

import com.github.rodrigopr.recsys.{StatsHolder, DataStore, Task}
import com.typesafe.config.Config
import edu.ucla.sspace.matrix._
import edu.ucla.sspace.vector.VectorMath
import util.Random
import scala.math._

object MatrixFactorizer extends Task {
  def execute(config: Config) = {
    val algorithm = config.getString("algorithm")

    if (algorithm.equalsIgnoreCase("mf") || algorithm.equalsIgnoreCase("hybrid-mf-item")) {
      val maxMovieId = DataStore.movies.keySet.map(_.toInt).max
      val maxUserId = DataStore.userRatings.keySet.map(_.toInt).max

      var matrix = new SparseHashMatrix(maxUserId + 1, maxMovieId + 1)

      DataStore.userRatings.keysIterator.foreach{ userId =>
        val moviesRatings = DataStore.userRatings(userId)
        moviesRatings.keysIterator.foreach{ movieId =>
          matrix.set(userId.toInt, movieId.toInt, moviesRatings(movieId))
        }
      }

      val alpha = Option(config.getDouble("alpha")).getOrElse(0.005)
      val beta = Option(config.getDouble("alpha")).getOrElse(0.02)
      val features = Option(config.getInt("features")).getOrElse(20)
      val interactions = Option(config.getInt("interactions")).getOrElse(20)

      Console.println("Begin fatorization")
      val matrices = StatsHolder.timeIt("Matrix factorization time", print = true) {
        matrixFactorization(matrix, features, interactions, alpha, beta)
      }

      val U = matrices._1
      val Qt = matrices._2

      DataStore.userRatings.clear()
      matrix = null

      DataStore.setAproximationMatrix(Matrices.multiply(U, Qt))

    }
    true
  }

  def matrixFactorization(M: Matrix, k: Int, steps: Int = 10, alpha: Double = 0.005, beta: Double = 0.02): (Matrix, Matrix) = {
    val U = new ArrayMatrix(Array.ofDim[Double](M.rows, k))
    val Q = new ArrayMatrix(Array.ofDim[Double](M.columns, k))
    val rand = new Random(1234567890l)
    for (i <- 0.to( max(M.rows, M.columns))) {
      for (j <- 0.to(k-1)) {
        if (i < M.rows) {
          U.set(i, j, rand.nextDouble())
        }
        if (i < M.columns) {
          Q.set(i, j, rand.nextDouble())
        }
      }
    }

    0.to(steps).foreach{ step =>
      0.to(M.rows-1).foreach { i =>
        0.to(M.columns-1).foreach { j =>
          if (M.get(i, j) > 0) {
            val eij = M.get(i, j) - VectorMath.dotProduct(U.getRowVector(i), Q.getRowVector(j))
            0.to(k -1).foreach { case (l: Int) =>
              val Uij = U.get(i, l) + alpha * (2 * eij * Q.get(j, l) - beta * U.get(i, l))
              val Qij = Q.get(j, l) + alpha * (2 * eij * U.get(i, l) - beta * Q.get(j, l))
              U.set(i, l, Uij)
              Q.set(j, l, Qij)
            }
          }
        }
      }
    }
    (U, Matrices.transpose(Q))
  }

  def getNonInfiteData(value: Double): Double = value match {
    case v if v.isPosInfinity => Double.MaxValue
    case v if v.isNegInfinity => Double.MinValue
    case v => v
  }
}
