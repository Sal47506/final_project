import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import scala.util.hashing.MurmurHash3

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: main input_graph_path output_matching_path")
      sys.exit(1)
    }
    print("SUIIIII 1")
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("MatchingTask")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    print("SUIIIII 2")
    // Read the input graph
    val edges = sc.textFile(inputPath).map { line =>
      val parts = line.split(",")
      Edge(parts(0).toLong, parts(1).toLong, 1)
    }
    val graph = Graph.fromEdges(edges, defaultValue = 0)

    // Run Luby's algorithm to compute the MIS
    val misVertices = lubyalgo(graph)
    print("SUIIIII 3")
    // Convert the MIS into a matching
    val misBroadcast = sc.broadcast(misVertices)
    val matchingEdges = graph.edges.filter { edge =>
      misBroadcast.value.contains(edge.srcId) && !misBroadcast.value.contains(edge.dstId)
    }.map(edge => s"${edge.srcId},${edge.dstId}")
    print("SUIIIII 4")
    // Save the matching to the output file as a single CSV
    matchingEdges.coalesce(1).saveAsTextFile(outputPath)

    println(s"Matching saved to $outputPath")
    sc.stop()
  }

  // Luby's algorithm implementation
  def lubyalgo(graph: Graph[Int, Int], degreeThreshold: Int = 5000): Set[VertexId] = {
    def hashScore(id: VertexId): Double =
      (MurmurHash3.stringHash(id.toString).toDouble / Int.MaxValue).abs

    var g = graph.partitionBy(PartitionStrategy.EdgePartition2D)

    val prunedGraph = g.outerJoinVertices(g.degrees)((id, _, degOpt) =>
      if (degOpt.getOrElse(0) < degreeThreshold) 1 else -1
    ).subgraph(vpred = (_, attr) => attr != -1)

    var activeGraph = prunedGraph.mapVertices((id, _) => hashScore(id))
    var MIS = Set[VertexId]()

    // Persist the initial graph
    activeGraph.vertices.persist(StorageLevel.MEMORY_AND_DISK)
    activeGraph.edges.persist(StorageLevel.MEMORY_AND_DISK)

    while (activeGraph.numVertices > 0) {
      val candidateVertices = activeGraph.aggregateMessages[Boolean](
        sendMsg = triplet => {
          if (triplet.srcAttr < triplet.dstAttr)
            triplet.sendToDst(true)
          else if (triplet.dstAttr < triplet.srcAttr)
            triplet.sendToSrc(true)
        },
        mergeMsg = _ && _
      ).filter(!_._2).keys

      val selected = candidateVertices.persist(StorageLevel.MEMORY_AND_DISK)
      val selectedSet = selected.collect().toSet
      if (selectedSet.isEmpty) return MIS

      MIS ++= selectedSet

      val selectedBroadcast = activeGraph.vertices.sparkContext.broadcast(selectedSet)
      val toRemove = activeGraph.triplets
        .filter(t => selectedBroadcast.value.contains(t.srcId) || selectedBroadcast.value.contains(t.dstId))
        .flatMap(t => Iterator(t.srcId, t.dstId))
        .distinct()
        .collect()
        .toSet

      val toRemoveBroadcast = activeGraph.vertices.sparkContext.broadcast(toRemove)

      // Unpersist the old graph before creating a new one
      activeGraph.vertices.unpersist(blocking = false)
      activeGraph.edges.unpersist(blocking = false)

      activeGraph = activeGraph.subgraph(vpred = (id, _) => !toRemoveBroadcast.value.contains(id))
      activeGraph = activeGraph.mapVertices((id, _) => hashScore(id))

      // Persist the new graph
      activeGraph.vertices.persist(StorageLevel.MEMORY_AND_DISK)
      activeGraph.edges.persist(StorageLevel.MEMORY_AND_DISK)
    }

    MIS
  }
}