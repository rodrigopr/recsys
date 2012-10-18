package com.github.rodrigopr.recsys.tasks

import com.github.rodrigopr.recsys.datasets.{Rating, Movie, DataSetParser, GroupLens1kk}

import io.Source
import java.util.concurrent.atomic.AtomicInteger
import com.github.rodrigopr.recsys.Task
import com.typesafe.config.Config
import com.github.rodrigopr.recsys.utils.RedisUtil._

object DatasetImporter extends Task {
  var genres = Set[String]()

  def execute(config: Config) = {
    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(config.getInt("parallelSize"))
    val parsers: Map[String, DataSetParser] = Map(
      "1kk" -> GroupLens1kk
    )

    // read from configuration which parse will be used
    val datasetParser = parsers.get(Option(config.getString("type")).getOrElse("1kk")).get

    importGenre(datasetParser, "resources/genre.dat")
    importMovies(datasetParser, "resources/movies.dat")
    importRatings(datasetParser, "resources/r1.train")
    true
  }

  def importRatings(parser: DataSetParser, file: String) {
    val count = new AtomicInteger(0)

    // load all ratings to memory to faster parallel processing
    val lines = Source.fromFile(file).getLines().map(parser.parseRating).toTraversable

    lines.par.foreach{ case Rating(userId, movieId, rating) =>
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
    val lines = Source.fromFile(file).getLines().withFilter(!_.isEmpty)
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
    val lines = Source.fromFile(file).getLines().withFilter(!_.isEmpty).map(parser.parseMovie).toList

    // process in parallel each movie
    lines.par.foreach { case Movie(movieId, movieName, year, movieGenres) =>
      pool.withClient ( _.pipeline { client =>
        client.set(buildKey("movie", movieId), movieId)
        client.hmset("movies", Map("name" -> movieName, "year" -> year))

        movieGenres.foreach { genre =>
          client.sadd(buildKey("movie", movieId, "genres"), genre)
        }

        Console.println("finished movie - count: " + lineCount.incrementAndGet())
      })
    }
  }
}
