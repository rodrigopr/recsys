package com.github.rodrigopr.recsys.scripts

import java.io.{FileReader, File, BufferedReader}

import io.Source
import collection.mutable
import java.util.concurrent.atomic.AtomicInteger
import actors.threadpool.locks.ReentrantLock
import scala.collection.JavaConversions._

import org.neo4j.graphdb.Node

import com.github.rodrigopr.recsys.Relation
import java.util.concurrent.ConcurrentHashMap

object GraphImporter extends BaseGraphScript {

  val genreMap: mutable.ConcurrentMap[String, Node] = new ConcurrentHashMap[String, Node]()
  val movieMap: mutable.ConcurrentMap[Int, Node] = new ConcurrentHashMap[Int, Node]()
  val userMap: mutable.ConcurrentMap[Int, Node] = new ConcurrentHashMap[Int, Node]()
  val lock = new ReentrantLock()
  collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(1000)

  importGenre("resources/genre.dat")
  importMovies("resources/u.item")
  importRatings("resources/u1.base")

  def createUser(userId: Int): Node = {
    lock.lock()
    try {
      userMap.getOrElse(userId, {
        doTx { db =>
          val userNode = db.createNode
          userNode.setProperty("id", userId)
          indexUser.add(userNode, "userid", userId)

          userMap.put(userId, userNode)

          userNode
        }
      })
    } finally {
      lock.unlock()
    }
  }

  def importRatings(file: String) {
    var count = 0
    val lines = Source.fromFile(file).getLines().toTraversable
    lines.par.foreach(line => {
      val rating = line.split("::")

      val userId: Int = rating(0).toInt
      val userNode = userMap.getOrElse(userId, { createUser(userId) })
      val movieNode = movieMap.get(rating(1).toInt).get

      if(movieNode != null) {
        doTx { db =>
          val ratingRel = userNode.createRelationshipTo(movieNode, Relation.Rated)
          ratingRel.setProperty("rating", rating(2).toInt)
        }
      }
      count += 1
      Console.println("finished ratting: " + line + ", count: " + count)
    })
  }

  def importGenre(file: String) {
    val lines = Source.fromFile(file).getLines().withFilter(!_.isEmpty)
    lines.toList.par.foreach( line => {
      val genre = line.trim
      doTx { db =>
        val genreNode = db.createNode()
        genreNode.setProperty("genre", genre)
        indexGenre.add(genreNode, "genreName", genre)
        genreMap.put(genre, genreNode)
        Console.println("Created genre " + genre)
      }
    })
  }

  def importMovies(file: String) {
    val reader = new BufferedReader(new FileReader(new File(file)))

    val lineCount = new AtomicInteger(0)

    val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null).toList
    lines.par.foreach { line =>
      val movie = line.split("::")
      doTx { db =>
        val movieNode = db.createNode

        val movieId = movie(0)
        movieNode.setProperty("movieid", movieId.toInt)
        movieNode.setProperty("name", movie(1))

        indexMovie.add(movieNode, "movieid", movieId)

        for (genre <- movie(2).split("\\|")) {
          if(!genre.isEmpty) {
            val genreNode: Node = genreMap.get(genre).get
            movieNode.createRelationshipTo(genreNode, Relation.Genre)
          }
        }

        movieMap.put(movieId.toInt, movieNode)
        Console.println("finished line: " + line + ", count: " + lineCount.incrementAndGet())
      }
    }
  }
}
