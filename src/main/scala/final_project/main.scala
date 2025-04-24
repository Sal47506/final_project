package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import java.nio.file.{Files, Paths}
import scala.util.Try

object main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  // Converts a line "a,b" to a canonical Edge
  def line_to_canonical_edge(line: String): Edge[Int] = {
    val x = line.split(",")
    if (x(0).toLong < x(1).toLong)
      Edge(x(0).toLong, x(1).toLong, 1)
    else
      Edge(x(1).toLong, x(0).toLong, 1)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("final_project_main")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    if (args.length == 0) {
      println("Usage: main option={compute,verify} ...")
      sys.exit(1)
    }

    if (args(0) == "compute") {
      // Usage: main compute graph_path output_path
      // This will output a directory with a CSV file of matching edges (srcId,dstId)
      val startTimeMillis = System.currentTimeMillis()
      val graph_edges = sc.textFile(args(1)).map(line_to_canonical_edge)
      val graph = Graph.fromEdges(graph_edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val misVertices = CustomLuby.lubyalgo(graph)
      println(s"DEBUG: misVertices.size = ${misVertices.size}")
      // Optionally print a sample of the set:
      println(s"DEBUG: misVertices sample = ${misVertices.take(10).mkString(",")}")
      val misSet = sc.broadcast(misVertices)
      val matchingEdges = graph.edges.filter { edge =>
        misSet.value.contains(edge.srcId) && !misSet.value.contains(edge.dstId)
      }
      println(s"DEBUG: matchingEdges.count = ${matchingEdges.count()}")
      val outputPath = args(2)
      // Delete output directory if it exists
      Try {
        val outPath = Paths.get(outputPath)
        if (Files.exists(outPath)) {
          import scala.reflect.io.Directory
          val dir = new Directory(new java.io.File(outputPath))
          dir.deleteRecursively()
        }
      }
      matchingEdges
        .map(edge => s"${edge.srcId},${edge.dstId}")
        .coalesce(1)
        .saveAsTextFile(outputPath)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println(s"Luby's algorithm completed in $durationSeconds s.")
      println("==================================")
      println(s"Matching saved to $outputPath")
      // The output CSV can now be used as input to the Python scripts for further processing
      misSet.destroy()
      graph.unpersist(blocking = false)
      sc.stop()
    } else if (args(0) == "verify") {
      // Usage: main verify graph_path matching_path
      // This will verify that the matching_path is a valid matching in graph_path
      val graph_edges = sc.textFile(args(1)).map(line_to_canonical_edge)
      val matched_edges = sc.textFile(args(2)).map(line_to_canonical_edge)
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
    } else {
      println("Usage: main option={compute,verify} ...")
      sys.exit(1)
    }
  }
}

// === Workflow ===
// 1. Run: spark-submit ... main compute graph.csv output_dir
//    - This produces a CSV file (in output_dir) with matching edges.
// 2. Use the output CSV as input to the Python scripts below for further processing or conversion.
// 3. To verify a matching, run: spark-submit ... main verify graph.csv matching.csv
//    - matching.csv can be the output from the Python scripts or the raw output from compute.
