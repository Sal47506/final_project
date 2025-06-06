package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object matching_verifier {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("verifier")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    def line_to_canonical_edge(line: String): Edge[Int] = {
      val x = line.split(",")
      if (x(0).toLong < x(1).toLong)
        Edge(x(0).toLong, x(1).toLong, 1)
      else
        Edge(x(1).toLong, x(0).toLong, 1)
    }

    if (args.length == 1) {
      // Only graph path provided: generate matching and write to CSV
      val graph_edges = sc.textFile(args(0)).map(line_to_canonical_edge)
      val graphInt = Graph.fromEdges(graph_edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val graph = graphInt.mapVertices((id, _) => scala.util.Random.nextDouble())
      val misVertices = GraphAlgorithms.bipartiteAlgo(graph) // Updated reference
      println(s"DEBUG: misVertices.size = ${misVertices.size}")
      val misBroadcast = sc.broadcast(misVertices)
      val matchingEdges = graph.edges.filter { edge =>
        misBroadcast.value.contains(edge.srcId) && !misBroadcast.value.contains(edge.dstId)
      }
      println(s"DEBUG: matchingEdges.count = ${matchingEdges.count()}")
      val outputPath = args(0) + "_matching_output"
      matchingEdges
        .map(edge => s"${edge.srcId},${edge.dstId}")
        .coalesce(1)
        .saveAsTextFile(outputPath)
      println(s"Matching saved to $outputPath")
      misBroadcast.destroy()
      graph.unpersist(blocking = false)
      sc.stop()
      sys.exit(0)
    }

    if (args.length != 2) {
      println("Usage: verifier graph_path [matching_path]")
      sys.exit(1)
    }

    val graph_edges = sc.textFile(args(0)).map(line_to_canonical_edge)
    val matched_edges = sc.textFile(args(1)).map(line_to_canonical_edge)

    if (matched_edges.distinct.count != matched_edges.count) {
      println("The matched edges contains duplications of an edge.")
      sys.exit(1)
    }

    if (matched_edges.intersection(graph_edges).count != matched_edges.count) {
      println("The matched edges are not a subset of the input graph.")
      sys.exit(1)
    }

    val matched_graph = Graph.fromEdges[Int, Int](matched_edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
    if (matched_graph.ops.degrees.aggregate(0)((x, v) => scala.math.max(x, v._2), (x, y) => scala.math.max(x, y)) >= 2) {
      println("The matched edges do not form a matching.")
      sys.exit(1)
    }
    println("The matched edges form a matching of size: " + matched_edges.count)
    sc.stop()
  }
}
