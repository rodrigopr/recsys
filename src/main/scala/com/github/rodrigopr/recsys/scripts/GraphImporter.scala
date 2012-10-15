package com.github.rodrigopr.recsys.scripts

import java.io.{FileReader, File, BufferedReader}

import io.Source
import java.util.concurrent.atomic.AtomicInteger

object GraphImporter extends BaseGraphScript {
  collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(100)
  var genres = Set[String]()

  importGenre("resources/genre.dat")
  importMovies("resources/movies.dat")
  importRatings("resources/r1.train")

  def importRatings(file: String) {
    var count = 0
    val lines = Source.fromFile(file).getLines().toTraversable
    lines.par.foreach(line => {
      val rating = line.split("::")

      val userId = rating(0)
      val movieId = rating(1)

      pool.withClient{ client =>
        client.zadd(buildKey("ratings", "user", userId), rating(2).toDouble, movieId)
        client.zadd(buildKey("ratings", "movie", movieId), rating(2).toDouble, userId)
        client.sadd("users", userId)
      }

      count += 1
      Console.println("finished ratting: " + line + ", count: " + count)
    })
  }

  def importGenre(file: String) {
    val lines = Source.fromFile(file).getLines().withFilter(!_.isEmpty)
    lines.toList.par.foreach( line => {
      val genre = line.trim
      pool.withClient { client =>
        client.sadd("genres", genre)
        Console.println("Created genre " + genre)
      }
    })

    pool.withClient(client => {
      genres = client.smembers("genres").get.map(_.get)
    })
  }

  def importMovies(file: String) {
    val reader = new BufferedReader(new FileReader(new File(file)))

    val lineCount = new AtomicInteger(0)

    val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null).toTraversable
    lines.par.foreach { line =>
      val movie = line.split("::")
      pool.withClient ( _.pipeline { client =>
        val movieId = movie(0)
        client.set(buildKey("movie", movieId), movie(1))
        client.sadd("movies", movieId)

        val movieGenres = movie(2).split("\\|").filter(genres.contains(_))
        movieGenres.foreach { genre =>
          client.sadd(buildKey("movie", movieId, "genres"), genre)
        }

        Console.println("finished line: " + line + ", count: " + lineCount.incrementAndGet())
      })
    }
  }
}
