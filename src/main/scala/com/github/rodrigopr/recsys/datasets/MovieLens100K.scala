package com.github.rodrigopr.recsys.datasets

import collection.mutable

object MovieLens100K extends DataSetParser {
  var genres = mutable.Map[Int,String]()
  
  val occupations  = List(
    "administrator", "artist", "doctor", "educator", "engineer",
    "entertainment", "executive", "healthcare", "homemaker", "lawyer",
    "librarian", "marketing", "none", "other", "programmer", "retired",
    "salesman", "scientist", "student", "technician", "writer")

  def parseGenre(genreLine: String) = {
    val genre = genreLine.trim.split('|')
    val genreName = genre(0)
    val genreId = genre(1)
    genres.put(genreId.toInt, genreName)

    Genre(genreName, genreName)
  }

  def parseMovie(movieLine: String) = {
    val movieSplit = movieLine.split("\\|")
    val year = if (movieSplit(2).isEmpty) 2000 else movieSplit(2).split("-").last.toInt
    val movieGenres = 1.to(18).filter(id => movieSplit(id + 5).equals("1")).map(genres)

    Movie(movieSplit(0), movieSplit(1), year, movieGenres)
  }

  def parseRating(line: String) = {
    val rating = line.split("\t")
    Rating(rating(0), rating(1), rating(2).toDouble)
  }

  def parseUser(userLine: String) = {
    val user = userLine.split('|')
    User(
      user(0),
      user(1).toInt,
      user(2),
      occupations.indexOf(user(3))
    )
  }
}
