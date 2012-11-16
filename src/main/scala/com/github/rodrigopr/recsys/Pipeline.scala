package com.github.rodrigopr.recsys

import com.typesafe.config.ConfigFactory
import tasks.{Recommender, DatasetImporter, NeighborSelection, ClusterBuilder}
import utils.Memoize
import java.io.File
import scala.collection.JavaConversions._
import scalax.io._

object Pipeline extends App {
  val defaultPipe = List (
    ("importer" -> DatasetImporter),
    ("clusterer" -> ClusterBuilder),
    ("neighbor-selection" -> NeighborSelection),
    ("recommender" -> Recommender)
  )

  val config = ConfigFactory.parseFile(new File("pipe.conf")).resolve()

  val casesConfig = config.getConfig("cases")

  casesConfig.root().entrySet().foreach{ caseName =>
    DataStore.clear()
    StatsHolder.clear()

    val caseNameStr = caseName.getKey
    StatsHolder.setCustomData("testName", caseNameStr)

    val currentCase = casesConfig.getConfig(caseNameStr)

    defaultPipe.takeWhile{ case (pipeName, task) =>
      Memoize.clean()
      task.execute(currentCase.getConfig(pipeName).withFallback(currentCase))
    }

    // Print to console
    StatsHolder.printAll(Console.print)

    // Save to unique log
    val output = Resource.fromFile("logs/%s.log".format(caseNameStr))
    for{
      processor <- output.outputProcessor
      out = processor.asOutput
    } {
      StatsHolder.printAll(out.write)
    }
  }
}
