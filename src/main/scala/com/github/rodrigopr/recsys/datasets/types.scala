package com.github.rodrigopr.recsys.datasets

case class Movie(id: String, name: String, year: Int, genre: Seq[String])
case class Rating(userId: String, movieId: String, rating: Double)
case class Genre(id: String, name: String)
