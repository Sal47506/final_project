package final_project

import org.apache.spark.graphx._
import scala.util.hashing.MurmurHash3
import org.apache.spark.storage.StorageLevel

object CustomLuby {
  def lubyalgo(graph: Graph[Int, Int], degreeThreshold: Int = 5000): Set[VertexId] = {
    // Assign a hash-based score to each vertex
    def hashScore(id: VertexId): Double =
      (MurmurHash3.stringHash(id.toString).toDouble / Int.MaxValue).abs

    // Partition and cache initial graph
    var g = graph.partitionBy(PartitionStrategy.EdgePartition2D)
    g.persist(StorageLevel.MEMORY_AND_DISK)

    // Prune high-degree vertices
    val prunedGraph = g.outerJoinVertices(g.degrees)((id, _, degOpt) =>
      if (degOpt.getOrElse(0) < degreeThreshold) 1 else -1
    ).subgraph(vpred = (_, attr) => attr != -1)
    
    g.unpersist(blocking = false)

    var activeGraph = prunedGraph.mapVertices((id, _) => hashScore(id))
    activeGraph.persist(StorageLevel.MEMORY_AND_DISK)
    var MIS = Set[VertexId]()

    while (activeGraph.numVertices > 0) {
      val messages = activeGraph.aggregateMessages[Boolean](
        sendMsg = triplet => {
          if (triplet.srcAttr < triplet.dstAttr)
            triplet.sendToDst(true)
          else if (triplet.dstAttr < triplet.srcAttr)
            triplet.sendToSrc(true)
        },
        mergeMsg = _ && _,
        TripletFields.All
      )

      val candidateVertices = messages.filter(!_._2).keys
      // Avoid collect on large RDDs; use mapPartitions if possible
      val selected = candidateVertices.persist(StorageLevel.MEMORY_AND_DISK)
      val selectedCount = selected.count()
      if (selectedCount == 0) {
        activeGraph.unpersist(blocking = false)
        selected.unpersist(blocking = false)
        return MIS
      }

      // If selectedCount is small, collect; else, consider alternate approaches
      val selectedSet = if (selectedCount < 1000000) selected.collect().toSet else {
        // For very large sets, write to disk or use a distributed join
        selected.map(id => id).collect().toSet // fallback, but not scalable for huge sets
      }
      MIS ++= selectedSet

      val oldGraph = activeGraph
      // Only broadcast if selectedSet is small
      val selectedBroadcast = if (selectedSet.size < 1000000)
        Some(activeGraph.vertices.sparkContext.broadcast(selectedSet))
      else None

      val toRemove = if (selectedBroadcast.isDefined) {
        activeGraph.triplets
          .filter(t => selectedBroadcast.get.value.contains(t.srcId) || selectedBroadcast.get.value.contains(t.dstId))
          .flatMap(t => Iterator(t.srcId, t.dstId))
          .distinct()
      } else {
        // For large sets, use a join instead of broadcast
        val selectedIds = selected.map(id => (id, null))
        activeGraph.triplets
          .flatMap(t => Iterator(t.srcId, t.dstId))
          .distinct()
          .map(id => (id, null))
          .join(selectedIds)
          .map(_._1)
      }
      val toRemoveSet = toRemove.collect().toSet

      selectedBroadcast.foreach(_.destroy())
      val toRemoveBroadcast = activeGraph.vertices.sparkContext.broadcast(toRemoveSet)

      activeGraph = activeGraph.subgraph(vpred = (id, _) => !toRemoveBroadcast.value.contains(id))
      activeGraph = activeGraph.mapVertices((id, _) => hashScore(id))
      activeGraph.persist(StorageLevel.MEMORY_AND_DISK)

      oldGraph.unpersist(blocking = false)
      selected.unpersist(blocking = false)
      toRemoveBroadcast.destroy()
    }

    activeGraph.unpersist(blocking = false)
    MIS
  }
}
// For further scalability, consider using GraphFrames or partitioning the graph more aggressively.
