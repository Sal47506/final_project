package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import scala.util.Try

object main {
  // Converts a line "a,b" to a canonical Edge
  def line_to_canonical_edge(line: String): Edge[Int] = {
    val x = line.split(",")
    if (x(0).toLong < x(1).toLong)
      Edge(x(0).toLong, x(1).toLong, 1)
    else
      Edge(x(1).toLong, x(0).toLong, 1)
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: main compute graph_path output_path algorithm={bipartitealgo,alonitai}")
      sys.exit(1)
    }

    val conf = new SparkConf().setAppName("final_project_main")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val algorithm = args(3).toLowerCase
    val graph_edges = sc.textFile(args(1)).map(line_to_canonical_edge)

    // Partition vertices into N groups (bipartite subgraphs)
    val numPartitions = 4
    def partitionId(id: Long): Int = (id % numPartitions).toInt

    // Assign each edge to a bipartite subgraph based on src/dst partition
    val bipartiteEdges = graph_edges.map { edge =>
      val p1 = partitionId(edge.srcId)
      val p2 = partitionId(edge.dstId)
      val part = if (p1 < p2) (p1, p2) else (p2, p1)
      (part, edge)
    }.groupByKey(numPartitions)

    // Collect grouped edges to driver, process each subgraph, then parallelize results
    println(s"DEBUG: Number of bipartite subgraphs: ${bipartiteEdges.count()}")
    val localMatchingEdges = bipartiteEdges.collect().flatMap { case ((p1, p2), edges) =>
      val edgeSeq = edges.toSeq
      println(s"DEBUG: Processing subgraph ($p1,$p2) with ${edgeSeq.size} edges")
      val subGraphInt = Graph.fromEdges(sc.parallelize(edgeSeq), 0)
      val subGraph = subGraphInt.mapVertices((id, _) => scala.util.Random.nextDouble())
      val selectedSources = algorithm match {
        case "bipartitealgo" =>
          println("DEBUG: Running bipartiteAlgo...")
          GraphAlgorithms.bipartiteAlgo(subGraph)
        case "alonitai" =>
          println("DEBUG: Running alonItaiMIS...")
          GraphAlgorithms.alonItaiMIS(subGraph)
        case _ =>
          println(s"ERROR: Unknown algorithm '$algorithm'. Use 'bipartitealgo' or 'alonitai'.")
          sys.exit(1)
      }
      println(s"DEBUG: Found ${selectedSources.size} selected source vertices")
      
      // Only keep edges where source is selected
      val matchedEdges = edgeSeq.filter(e => selectedSources.contains(e.srcId))
      println(s"DEBUG: Found ${matchedEdges.size} matching edges for subgraph ($p1,$p2)")
      matchedEdges
    }

    // Ensure we have edges before proceeding
    if (localMatchingEdges.isEmpty) {
      println("WARNING: No matching edges found in any subgraph!")
      sys.exit(1)
    }

    println(s"DEBUG: Total matching edges found: ${localMatchingEdges.length}")

    // === GLOBAL FILTERING TO ENSURE MATCHING ===
    val usedVertices = scala.collection.mutable.Set[Long]()
    val globallyFilteredEdges = localMatchingEdges.filter { e =>
      if (!usedVertices.contains(e.srcId) && !usedVertices.contains(e.dstId)) {
        usedVertices += e.srcId
        usedVertices += e.dstId
        true
      } else {
        false
      }
    }

    val matchingEdges = sc.parallelize(globallyFilteredEdges)

    // Force count before saving to ensure we have data
    val finalCount = matchingEdges.count()
    println(s"DEBUG: Final matching size: $finalCount")
    
    if (finalCount == 0) {
      println("ERROR: No matching edges to write!")
      sys.exit(1)
    }

    val outputPath = args(2)
    // Delete output directory if it exists
    Try {
      val outPath = new java.io.File(outputPath)
      if (outPath.exists()) {
        import scala.reflect.io.Directory
        new Directory(outPath).deleteRecursively()
      }
    }
    matchingEdges
      .map(edge => s"${edge.srcId},${edge.dstId}")
      .coalesce(1)
      .saveAsTextFile(outputPath)

    println(s"Matching saved to $outputPath")
    sc.stop()
  }
}
