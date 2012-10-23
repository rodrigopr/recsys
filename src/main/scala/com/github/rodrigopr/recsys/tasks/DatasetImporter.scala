package com.github.rodrigopr.recsys.tasks

import com.github.rodrigopr.recsys.datasets._

import io.Source
import java.util.concurrent.atomic.AtomicInteger
import com.github.rodrigopr.recsys.Task
import com.typesafe.config.Config
import com.github.rodrigopr.recsys.utils.RedisUtil._
import com.github.rodrigopr.recsys.datasets.Movie
import com.github.rodrigopr.recsys.datasets.Rating

object DatasetImporter extends Task {
  var genres = Set[String]()

  def execute(config: Config) = {
    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(config.getInt("parallelism"))

    val datasetParser = getDatasetParser(config)
    val prefix = Option(config.getString("resource-prefix")).getOrElse("resources/")

    importGenre(datasetParser, prefix + "genre.dat")
    importMovies(datasetParser, prefix + "movies.dat")
    importRatings(datasetParser, prefix + "r1.train")
    true
  }


  def getDatasetParser(config: Config): DataSetParser = {
    val parsers: Map[String, DataSetParser] = Map(
      "10M" -> MovieLens10M,
      "100k" -> MovieLens100K
    )

    // read from configuration which parse will be used
    val datasetParser = parsers.get(Option(config.getString("type")).getOrElse("10M")).get
    datasetParser
  }

  def importRatings(parser: DataSetParser, file: String) {
    val count = new AtomicInteger(0)

    // load all ratings to memory to faster parallel processing
    val lines = Source.fromFile(file, "utf-8").getLines().withFilter(!_.isEmpty).toSeq

    lines.par.map(parser.parseRating).foreach{ case Rating(userId, movieId, rating) =>
      pool.withClient( _.pipeline { client =>
        // add user rating
        client.zadd(buildKey("ratings", "user", userId), rating, movieId)
        // add reverse user rating
        client.zadd(buildKey("ratings", "movie", movieId), rating, userId)
        // add the user, if no exists
        client.sadd("users", userId)
      })

      count.incrementAndGet()
      Console.println("finished ratting - count: " + count.get)
    }
  }

  def importGenre(parser: DataSetParser, file: String) {
    val lines = Source.fromFile(file, "utf-8").getLines().withFilter(!_.isEmpty)
    lines.map(parser.parseGenre).toList.foreach( genre => {
      pool.withClient { client =>
        client.sadd("genres", genre.name)
        Console.println("Created genre " + genre)
      }
    })
  }

  def importMovies(parser: DataSetParser, file: String) {
    val lineCount = new AtomicInteger(0)

    // load the whole data to memory
    val lines = Source.fromFile(file, "ISO-8859-1").getLines().withFilter(!_.isEmpty).toList

    // process in parallel each movie
    lines.par.map(parser.parseMovie).foreach { case Movie(movieId, movieName, year, movieGenres) =>
      pool.withClient ( _.pipeline { client =>
        client.set(buildKey("movie", movieId, "name"), movieName)
        client.set(buildKey("movie", movieId, "year"), year.toString)
        client.sadd(buildKey("movies"), movieId)

        movieGenres.foreach { genre =>
          client.sadd(buildKey("movie", movieId, "genres"), genre)
        }

        Console.println("finished movie - count: " + lineCount.incrementAndGet())
      })
    }
  }
}
