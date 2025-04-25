package final_project

import org.apache.spark.graphx._
import scala.util.Random

object CustomLuby {
  def lubyalgo(graph: Graph[Double, Int], selectProb: Double = 1.0): Set[VertexId] = {
    // Get edges and vertices with weights
    val edges = graph.edges.collect()
    val vertices = graph.vertices.collect().toMap
    
    var matching = Set[VertexId]()
    var matchedVertices = Set[VertexId]()  // Track all matched vertices (both src and dst)
    
    // Sort edges by source vertex weight
    val sortedEdges = edges.sortBy(e => vertices.getOrElse(e.srcId, 0.0))
    
    // Greedy matching ensuring each vertex appears at most once
    for (edge <- sortedEdges) {
      // Only add edge if neither endpoint is matched
      if (!matchedVertices.contains(edge.srcId) && !matchedVertices.contains(edge.dstId)) {
        matching += edge.srcId  // Add source vertex to matching
        matchedVertices += edge.srcId
        matchedVertices += edge.dstId
      }
    }
    
    matching
  }
}
