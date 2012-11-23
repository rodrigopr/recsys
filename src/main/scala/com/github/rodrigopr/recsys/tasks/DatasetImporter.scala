package com.github.rodrigopr.recsys.tasks

import com.github.rodrigopr.recsys.datasets._

import io.Source
import java.util.concurrent.atomic.AtomicInteger
import com.github.rodrigopr.recsys.{DataStore, StatsHolder, Task}
import com.typesafe.config.Config

object DatasetImporter extends Task {
  def execute(config: Config) = {
    collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(config.getInt("parallelism"))

    val datasetParser = getDatasetParser(config)
    val prefix = Option(config.getString("resource-prefix")).getOrElse("resources/")

    val ratingData = Option(config.getString("rating-data")).getOrElse("1")

    val algorithm = config.getString("algorithm")

    importGenre(datasetParser, prefix + "genre.dat")
    importMovies(datasetParser, prefix + "movies.dat")
    importUsers(datasetParser, prefix + "users.dat")
    importRatings(datasetParser, prefix + "r" + ratingData + ".train", algorithm)
    true
  }

  def getDatasetParser(config: Config): DataSetParser = {
    val parsers: Map[String, DataSetParser] = Map(
      "10M" -> MovieLens10M,
      "100K" -> MovieLens100K
    )

    // read from configuration which parse will be used
    val datasetParser = parsers.get(Option(config.getString("type")).getOrElse("10M")).get
    datasetParser
  }

  def importRatings(parser: DataSetParser, file: String, algorithm: String) {
    val count = new AtomicInteger(0)

    // load all ratings to memory to faster parallel processing
    val lines = Source.fromFile(file, "utf-8").getLines().withFilter(!_.isEmpty)

    lines.map(parser.parseRating).foreach{ rating =>
      DataStore.registerRating(rating, !algorithm.equals("mf"))

      count.incrementAndGet()
      StatsHolder.incr("Ratings")
      Console.println("finished rating - count: " + count.get)
    }

    DataStore.calcUserAvgRating()
  }

  def importGenre(parser: DataSetParser, file: String) {
    val lines = Source.fromFile(file, "utf-8").getLines().withFilter(!_.isEmpty)
    lines.map(parser.parseGenre).toList.foreach( genre => {
      DataStore.genres.add(genre)
      StatsHolder.incr("Genres")
      Console.println("Created genre " + genre)
    })
  }

  def importUsers(parser: DataSetParser, file: String) {
    val lines = Source.fromFile(file, "utf-8").getLines().withFilter(!_.isEmpty)
    lines.map(parser.parseUser).toList.foreach( user => {
      DataStore.users.put(user.id, user)

      StatsHolder.incr("Users")
      Console.println("Import user " + user.id)
    })
  }

  def importMovies(parser: DataSetParser, file: String) {
    val lineCount = new AtomicInteger(0)

    val lines = Source.fromFile(file, "ISO-8859-1").getLines().withFilter(!_.isEmpty)

    // process in parallel each movie
    lines.map(parser.parseMovie).foreach { case movie =>
      DataStore.movies.put(movie.id, movie)
      DataStore.movieRatings.put(movie.id, Map[String, Double]())

      StatsHolder.incr("Movies")
      Console.println("finished movie - count: " + lineCount.incrementAndGet())
    }
  }
}
