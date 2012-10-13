package com.github.rodrigopr.recsys.scripts

import scala.collection.JavaConversions._
import com.github.rodrigopr.recsys.Relation

class ClusterIndex extends BaseGraphScript {
  collection.parallel.ForkJoinTasks.defaultForkJoinPool.setParallelism(200)

  indexUser.query("userid", "*").iterator().toTraversable.par.foreach(user => {
    user.getRelationships(Relation.InCluster).foreach(rel => indexInCLuster.add(rel.getEndNode, "nodeId", user.getId))
  })
}
