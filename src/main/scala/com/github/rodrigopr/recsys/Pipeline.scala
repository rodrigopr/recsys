package com.github.rodrigopr.recsys

import com.typesafe.config.{ConfigValue, ConfigFactory}
import tasks.{Recommender, DatasetImporter, NeighborSelection, ClusterBuilder}
import utils.Memoize
import java.io.File
import com.github.rodrigopr.recsys.utils.RedisUtil._
import java.util
import util.Map.Entry

object Pipeline extends App {
  val defaultPipe = List (
    ("importer" -> DatasetImporter),
    ("clusterer" -> ClusterBuilder),
    ("neighbor-selection" -> NeighborSelection),
    ("recommender" -> Recommender)
  )

  pool.withClient(_.flushall)
  StatsHolder.clear()

  val config = ConfigFactory.parseFile(new File("pipe.conf")).getConfig("pipe")
  val componentsConfig = config.getConfig("components")

  defaultPipe.takeWhile{ case (pipeName, task) =>
    Memoize.clean()
    task.execute(componentsConfig.getConfig(pipeName).withFallback(config))
  }

  //0val x: util.Set[Entry[String, ConfigValue]] = config.root().entrySet()
  //x.iterator().map

  StatsHolder.printAll()
}
