package com.github.rodrigopr.recsys

import com.typesafe.config.ConfigFactory
import tasks.{DatasetImporter, NeighborSelection, ClusterBuilder}
import utils.Memoize
import java.io.File

object Pipeline extends App {
  val defaultPipe = List (
    ("importer" -> DatasetImporter),
    ("clusterer" -> ClusterBuilder),
    ("neighbor-selection" -> NeighborSelection)
  )

  val config = ConfigFactory.parseFile(new File("pipe.conf")).getConfig("pipe")
  val componentsConfig = config.getConfig("components")

  defaultPipe.takeWhile{ case (pipeName, task) =>
    Memoize.clean()
    task.execute(componentsConfig.getConfig(pipeName).withFallback(config))
  }
}
