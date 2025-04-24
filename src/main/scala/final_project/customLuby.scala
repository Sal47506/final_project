import org.apache.spark.graphx._
import scala.util.hashing.MurmurHash3
import org.apache.spark.storage.StorageLevel

object CustomLuby {
  def lubyalgo(graph: Graph[Int, Int], degreeThreshold: Int = 5000): Set[VertexId] = {
    // Assign a hash-based score to each vertex
    def hashScore(id: VertexId): Double =
      (MurmurHash3.stringHash(id.toString).toDouble / Int.MaxValue).abs

    // Partition graph to reduce shuffle
    var g = graph.partitionBy(PartitionStrategy.EdgePartition2D)

    // Prune high-degree vertices
    val prunedGraph = g.outerJoinVertices(g.degrees)((id, _, degOpt) =>
      if (degOpt.getOrElse(0) < degreeThreshold) 1 else -1
    ).subgraph(vpred = (_, attr) => attr != -1)

    var activeGraph = prunedGraph.mapVertices((id, _) => hashScore(id))
    var MIS = Set[VertexId]()

    activeGraph.vertices.persist(StorageLevel.MEMORY_AND_DISK)
    activeGraph.edges.persist(StorageLevel.MEMORY_AND_DISK)

    while (activeGraph.numVertices > 0) {
      // Identify local minima in neighborhood
      val candidateVertices = activeGraph.aggregateMessages[Boolean](
        sendMsg = triplet => {
          if (triplet.srcAttr < triplet.dstAttr)
            triplet.sendToDst(true)
          else if (triplet.dstAttr < triplet.srcAttr)
            triplet.sendToSrc(true)
        },
        mergeMsg = _ && _
      ).filter(!_._2).keys // only keep those not beaten by any neighbor

      val selected = candidateVertices.persist(StorageLevel.MEMORY_AND_DISK)
      val selectedSet = selected.collect().toSet
      if (selectedSet.isEmpty) return MIS

      MIS ++= selectedSet

      // Get neighbors of selected vertices
      val selectedBroadcast = activeGraph.vertices.sparkContext.broadcast(selectedSet)
      val toRemove = activeGraph.triplets
        .filter(t => selectedBroadcast.value.contains(t.srcId) || selectedBroadcast.value.contains(t.dstId))
        .flatMap(t => Iterator(t.srcId, t.dstId))
        .distinct()
        .collect()
        .toSet

      val toRemoveBroadcast = activeGraph.vertices.sparkContext.broadcast(toRemove)
      activeGraph = activeGraph.subgraph(vpred = (id, _) => !toRemoveBroadcast.value.contains(id))
      activeGraph = activeGraph.mapVertices((id, _) => hashScore(id))
    }

    MIS
  }
}