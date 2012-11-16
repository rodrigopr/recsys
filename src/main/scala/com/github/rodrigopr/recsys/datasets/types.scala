package com.github.rodrigopr.recsys.datasets

case class Movie(id: String, name: String, year: Int, genre: Seq[String])
case class Rating(userId: String, movieId: String, rating: Double)
case class Genre(id: String, name: String)
case class User(id: String, age: Int, gender: String, occupation: Int)
case class Neighbour(id: String, similarity: Double)
