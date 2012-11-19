package com.github.rodrigopr.recsys

import com.typesafe.config.ConfigFactory
import tasks._
import utils.Memoize
import java.io.File
import scala.collection.JavaConversions._
import scalax.io._

object Pipeline extends App {
  val defaultPipe = List (
    ("importer" -> DatasetImporter),
    //("matrix-factorizer" -> MatrixFactorizer),
    ("clusterer" -> ClusterBuilder),
    ("neighbor-selection" -> NeighborSelection),
    ("recommender" -> Recommender)
  )

  val config = ConfigFactory.parseFile(new File("pipe.conf")).resolve()

  val casesConfig = config.getConfig("cases")

  casesConfig.root().keySet().toList.sorted.foreach{ caseName =>
    DataStore.clear()
    StatsHolder.clear()

    StatsHolder.setCustomData("testName", caseName)

    val currentCase = casesConfig.getConfig(caseName)

    defaultPipe.takeWhile{ case (pipeName, task) =>
      Memoize.clean()
      task.execute(currentCase.getConfig(pipeName).withFallback(currentCase))
    }

    // Print to console
    StatsHolder.printAll(Console.print)

    // Save to unique log
    val output = Resource.fromFile("logs/%s.log".format(caseName))
    for{
      processor <- output.outputProcessor
      out = processor.asOutput
    } {
      StatsHolder.printAll(out.write)
    }
  }
}
