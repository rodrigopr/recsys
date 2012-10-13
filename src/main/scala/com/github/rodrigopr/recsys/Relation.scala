package com.github.rodrigopr.recsys

import org.neo4j.graphdb.RelationshipType

object Relation extends Enumeration {

  type Relation = Value

  val Neighbor = Value
  val Watched = Value
  val Rated = Value
  val Genre = Value
  val InCluster = Value

  implicit def toRelType(rel: Relation) = new RelationshipType {
    def name = rel.toString
  }
}