package com.github.rodrigopr.recsys.scripts

import java.io.{FileReader, File, BufferedReader}

import io.Source
import collection.mutable

import org.neo4j.graphdb.Node

import com.github.rodrigopr.recsys.Relation
import java.util.concurrent.atomic.AtomicInteger

object GraphImporter extends BaseGraphScript {

  val genreMap = mutable.HashMap[Int, String]()

  importGenre("resources/u.genre")
  importMovies("resources/u.item")
  importRatings("resources/u1.base")

  def importRatings(file: String) {
    var count = 0
    val lines = Source.fromFile(file).getLines().toTraversable
    lines.par.foreach(line => {
      val rating = line.split("\t")

      val userId: Int = rating(0).toInt
      var userNode = indexUser.get("userid", userId).getSingle
      val movieNode = indexMovie.get("movieid", rating(1).toInt).getSingle

      if(movieNode != null) {
        doTx { db =>
          if(userNode == null) {
            userNode = db.createNode
            userNode.setProperty("id", userId)
            indexUser.add(userNode, "userid", userId)
          }

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
      val genre = line.split("\\|")
      doTx { db =>
        val genreNode = db.createNode()
        genreNode.setProperty("genre", genre(0))
        indexGenre.add(genreNode, "genreid", genre(1).toInt)
        Console.println("Created genre " + genre(0))
      }
      genreMap.put(genre(1).toInt, genre(0))
    })
  }

  def importMovies(file: String) {
    val reader = new BufferedReader(new FileReader(new File(file)))

    var lineCount = new AtomicInteger(0)

    val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null).toList
    lines.foreach { line =>
      val movie = line.split("\\|")
      doTx { db =>
        val movieNode = db.createNode

        val movieId = movie(0)
        movieNode.setProperty("movieid", movieId)
        movieNode.setProperty("name", movie(1))

        indexMovie.add(movieNode, "movieid", movieId)

        for (genreId <- 1.to(18)) {
          if(movie(genreId + 5).toInt == 1) {
            val genreNode: Node = indexGenre.get("genreid", genreId).getSingle
            movieNode.createRelationshipTo(genreNode, Relation.Genre)
          }
        }

        Console.println("finished line: " + line + ", count: " + lineCount.incrementAndGet())
      }
    }
  }
}
