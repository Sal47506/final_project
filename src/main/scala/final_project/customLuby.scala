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


  def alonItaiMIS(graph: Graph[Double, Int]): Set[VertexId] = {
    var g = graph
    var MIS = Set[VertexId]()

    println(s"DEBUG: alonItaiMIS initial vertices: ${g.vertices.count()}, edges: ${g.edges.count()}")

    // While the graph is not empty
    while (g.vertices.count() > 0) {
      // Phase: Select an independent set S
      val independentSet = inPhaseOptimized(g)
      MIS ++= independentSet

      // Remove S and its neighbors from the graph
      val verticesToRemove = g.aggregateMessages[Set[VertexId]](
        triplet => {
          if (independentSet.contains(triplet.srcId)) triplet.sendToDst(Set(triplet.srcId))
          if (independentSet.contains(triplet.dstId)) triplet.sendToSrc(Set(triplet.dstId))
        },
        _ ++ _
      ).keys.collect().toSet ++ independentSet

      g = g.subgraph(vpred = (id, _) => !verticesToRemove.contains(id)).cache()

      println(s"DEBUG: alonItaiMIS iteration complete, remaining vertices: ${g.vertices.count()}, edges: ${g.edges.count()}")
    }

    println(s"DEBUG: alonItaiMIS returning MIS size: ${MIS.size}")
    MIS
  }

  private def inPhaseOptimized(graph: Graph[Double, Int]): Set[VertexId] = {
    // Step 1: Mark vertices based on their degree and collect marking information
    val initialMarks = graph.aggregateMessages[Boolean](
      triplet => {
        val srcDegree = triplet.srcAttr
        val dstDegree = triplet.dstAttr
        if (srcDegree == 0 || Random.nextDouble() < 1.0 / srcDegree) triplet.sendToSrc(true)
        if (dstDegree == 0 || Random.nextDouble() < 1.0 / dstDegree) triplet.sendToDst(true)
      },
      _ || _
    ).collectAsMap()

    // Step 2: Create a broadcast variable for marked vertices
    val markedVerticesBroadcast = graph.vertices.context.broadcast(initialMarks)

    // Step 3: Resolve conflicts in a single pass
    val resolvedVertices = graph.aggregateMessages[Boolean](
      triplet => {
        val srcMarked = markedVerticesBroadcast.value.getOrElse(triplet.srcId, false)
        val dstMarked = markedVerticesBroadcast.value.getOrElse(triplet.dstId, false)
        
        if (srcMarked && dstMarked) {
          // If both endpoints are marked, unmark one based on random choice
          if (Random.nextDouble() < 0.5) triplet.sendToSrc(false)
          else triplet.sendToDst(false)
        }
      },
      _ || _
    ).collectAsMap()

    // Step 4: Compute final independent set
    val independentSet = initialMarks.filter { case (vid, marked) => 
      marked && !resolvedVertices.getOrElse(vid, false)
    }.keySet.toSet[VertexId] // Convert to immutable Set[VertexId]

    markedVerticesBroadcast.destroy()
    println(s"DEBUG: inPhaseOptimized selected independent set size: ${independentSet.size}")
    independentSet
  }

  def greedyMIS(graph: Graph[Double, Int]): Set[VertexId] = {
    var g = graph
    var MIS = Set[VertexId]()

    println(s"DEBUG: greedyMIS initial vertices: ${g.vertices.count()}, edges: ${g.edges.count()}")

    while (g.vertices.count() > 0) {
      // Check if degrees RDD is empty
      if (g.degrees.isEmpty()) {
        println("DEBUG: No vertices with degrees left in the graph.")
        return MIS
      }

      // Select the vertex with the highest degree
      val highestDegreeVertex = g.degrees.max()(Ordering.by(_._2))._1
      MIS += highestDegreeVertex

      // Remove the selected vertex and its neighbors
      val neighbors = g.edges
        .filter(e => e.srcId == highestDegreeVertex || e.dstId == highestDegreeVertex)
        .flatMap(e => Seq(e.srcId, e.dstId))
        .collect()
        .toSet

      g = g.subgraph(vpred = (id, _) => id != highestDegreeVertex && !neighbors.contains(id)).cache()

      println(s"DEBUG: greedyMIS iteration complete, remaining vertices: ${g.vertices.count()}, edges: ${g.edges.count()}")
    }

    println(s"DEBUG: greedyMIS returning MIS size: ${MIS.size}")
    MIS
  }

  private def sparsifiedMISAlgorithm(graph: Graph[Double, Int]): Set[VertexId] = {
    val delta = graph.degrees.map(_._2).reduce(math.max)
    val rounds = math.log(math.log(delta)).toInt.max(1)

    var g = graph.mapVertices((_, _) => Random.nextDouble())
    var MIS = Set[VertexId]()

    for (_ <- 1 to rounds) {
      val selected = g.vertices.filter(_._2 < 0.5).map(_._1).collect().toSet
      MIS ++= selected
      g = g.subgraph(vpred = (id, _) => !selected.contains(id))
        .cache()
    }

    MIS
  }
}
