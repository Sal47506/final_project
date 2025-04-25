package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

import scala.util.Random

object main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(lineGraph: Graph[Double, Int]): Set[VertexId] = {
    var graph = lineGraph
    var mis = Set[VertexId]()

    while (!graph.vertices.isEmpty()) {
      // Find the maximal weight of neighbors
      val maxNeighborWeight = graph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr),
        (a, b) => math.max(a, b)
      )

      // Select nodes (edges in the original graph) that have the highest weight
      val candidates = graph.vertices.leftOuterJoin(maxNeighborWeight)
        .filter { case (_, (selfWeight, maxNeighbor)) =>
          maxNeighbor.forall(nw => selfWeight > nw)
        }
        .map(_._1)
        .collect()
        .toSet

      // Add the selected edges to the MIS set
      mis ++= candidates

      // Get the edges that are adjacent to the selected ones and remove them
      val toRemove = graph.triplets
        .filter(triplet => candidates.contains(triplet.srcId) || candidates.contains(triplet.dstId))
        .flatMap(triplet => Seq(triplet.srcId, triplet.dstId))
        .distinct()
        .collect()
        .toSet

      // Broadcast the list of edges to remove
      val bcToRemove = graph.vertices.sparkContext.broadcast(toRemove)

      // Remove the conflicting edges from the graph (both edges in the line graph)
      graph = graph.subgraph(vpred = (id, _) => !bcToRemove.value.contains(id))
    }

    mis
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: final_project <graph_path> <output_path>")
      sys.exit(1)
    }

    val graphPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName("final_project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val rawEdges = sc.textFile(graphPath)
      .map(line => line.split(",").map(_.trim.toLong))
      .map { case Array(u, v) => if (u < v) (u, v) else (v, u) }
      .distinct()
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK)

    val edgeIdToEdge = rawEdges.map(_.swap).persist(StorageLevel.MEMORY_AND_DISK)

    val edgeNeighbors = rawEdges
      .flatMap { case ((u, v), eid) => Seq((u, eid), (v, eid)) }
      .groupByKey()
      .flatMap { case (_, eids) =>
        val eidList = eids.toArray.sorted
        for {
          i <- eidList.indices
          j <- (i + 1) until eidList.length
        } yield (eidList(i), eidList(j))
      }

    val lineGraph = Graph.fromEdgeTuples(edgeNeighbors, defaultValue = 1)
      .mapVertices((_, _) => Random.nextDouble())

    val matchingEdgeIds = LubyMIS(lineGraph)

    val matchedEdges = edgeIdToEdge.filter { case (eid, _) => matchingEdgeIds.contains(eid) }.map(_._2)

    matchedEdges
      .map { case (u, v) => s"$u,$v" }
      .coalesce(1) // ensure single output file
      .saveAsTextFile(outputPath)
  }
}