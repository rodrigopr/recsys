package com.github.rodrigopr.recsys.datasets

trait DataSetParser {
  def parseGenre(genreLine: String): Genre
  def parseMovie(movieLine: String): Movie
  def parseRating(ratingLine: String): Rating
}
