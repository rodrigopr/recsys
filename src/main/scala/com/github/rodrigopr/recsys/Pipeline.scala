package com.github.rodrigopr.recsys

import com.typesafe.config.ConfigFactory
import tasks.{DatasetImporter, NeighborSelection, ClusterBuilder}
import utils.Memoize

class Pipeline extends App {
  override def main(args: Array[String]) {
    val defaultPipe = List(
      ("importer" -> DatasetImporter),
      ("clusterer" -> ClusterBuilder)
    )

    val conf = ConfigFactory.load("pipeConf")

    defaultPipe.takeWhile{ case (pipeName, task) =>
      Memoize.clean()
      task.execute(conf.getConfig(pipeName))
    }
  }
}
