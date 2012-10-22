package com.github.rodrigopr.recsys.datasets

import collection.mutable

object MovieLens10M extends DataSetParser {
  var genres = mutable.Set[String]()

  def parseGenre(genreLine: String) = {
    val genreName = genreLine.trim
    genres.add(genreName)

    Genre(genreName, genreName)
  }

  def parseMovie(movieLine: String) = {
    val movieSplit = movieLine.split("::")
    val year = movieSplit(1).split(" ").last.replaceAll("[()]", "") match {
      case y if "12".contains(y.head) => y.toInt
      case _ => 0
    }
    val movieGenres = movieSplit(2).split("\\|").filter(genres.contains)

    Movie(movieSplit(0), movieSplit(1), year, movieGenres)
  }

  def parseRating(line: String) = {
    val rating = line.split("::")
    Rating(rating(0), rating(1), rating(2).toDouble)
  }
}
