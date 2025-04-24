package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    def LubyMIS(g_in: Graph[Int, Int]): (Graph[Int, Int], Int) = {
        val startTimeMillis = System.currentTimeMillis()

        var g = g_in.mapVertices((id, attr) => 0) // 0: undecided, 1: in MIS, -1: not in MIS
        var remaining_vertices = g.vertices.count()

        var iterations = 0
        while (remaining_vertices > 0) {
            iterations += 1
            // Generate random numbers for undecided vertices
            val random_g = g.mapVertices((id, attr) => 
                if (attr == 0) scala.util.Random.nextDouble() else -1.0
            )
            
            // Compare random values with neighbors and send messages
            val messages = random_g.aggregateMessages[Boolean](
                triplet => {
                if (triplet.srcAttr >= 0 && triplet.dstAttr >= 0) {
                    if (triplet.srcAttr > triplet.dstAttr) {
                    triplet.sendToSrc(true)
                    } else {
                    triplet.sendToSrc(false)
                    }

                    if (triplet.dstAttr > triplet.srcAttr) {
                    triplet.sendToDst(true)
                    } else {
                    triplet.sendToDst(false)
                    }
                }
                },
                (a, b) => a && b
            )
            
            // Update vertex states
            g = g.outerJoinVertices(messages) {
                case (id, oldAttr, Some(msg)) => 
                if (oldAttr == 0 && msg) 1 // Add to MIS if greater than all neighbors
                else oldAttr
                case (id, oldAttr, None) => 
                if (oldAttr == 0) 1 // Isolated vertices join MIS
                else oldAttr
            }
            
            // Mark neighbors of MIS vertices as not in MIS
            val neighbors = g.aggregateMessages[Int](
                triplet => {
                if (triplet.srcAttr == 1) triplet.sendToDst(-1)
                if (triplet.dstAttr == 1) triplet.sendToSrc(-1)
                },
                (a, b) => -1
            )
            
            g = g.outerJoinVertices(neighbors) {
                case (id, oldAttr, Some(-1)) => -1 
                case (id, oldAttr, _) => oldAttr 
            }
            
            remaining_vertices = g.vertices.filter(_._2 == 0).count()

            val endTimeMillis = System.currentTimeMillis()
            val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
            System.out.println(s"$iterations iterations complete in $durationSeconds sec. $remaining_vertices vertices remaining")
        }
        (g, iterations)
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("final_project")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder.config(conf).getOrCreate()

        if (args.length != 2) {
            println("Usage: matcher <graph_path> <output_file_name>")
            sys.exit(1)
        }

        val graph_path = args(0)
        val output_file_name = args(1)

        // Ensure the output file is saved in the "outputs" directory
        val output_path = s"outputs/$output_file_name"

        val graph_edges = sc.textFile(graph_path).map(line => {
            val x = line.split(",")
            Edge(x(0).toLong, x(1).toLong, 1)
        })

        // Check if the input graph is empty
        if (graph_edges.isEmpty()) {
            println("Error: Input graph is empty or invalid.")
            sys.exit(1)
        }

        val graph = Graph.fromEdges(graph_edges, defaultValue = 0)

        // Assign unique IDs to edges for the line graph
        val edgeWithId = graph.edges.zipWithIndex().map { case (edge, id) =>
            (id.toLong, (edge.srcId, edge.dstId))
        }

        // Convert to line graph
        val lineGraphEdges: RDD[Edge[Int]] = edgeWithId.flatMap { case (id, (srcId, dstId)) =>
            Seq(
                Edge(id.toInt, srcId.toInt, 1),
                Edge(id.toInt, dstId.toInt, 1)
            )
        }

        val lineGraph = Graph.fromEdges(lineGraphEdges, defaultValue = 0)

        // Run Luby's algorithm on the line graph
        val (misGraph, _) = LubyMIS(lineGraph)

        // Extract matching edges by joining with edgeWithId
        val matchingEdges = misGraph.vertices
            .filter(_._2 == 1) // Keep only vertices in the MIS
            .join(edgeWithId)  // Join with edgeWithId to get the original edges
            .map { case (_, (_, (srcId, dstId))) => (srcId, dstId) }

        // Save matching edges as a single CSV file
        matchingEdges
            .map { case (src, dst) => s"$src,$dst" }
            .coalesce(1) // Ensure a single output file
            .saveAsTextFile(output_path)

        println(s"Matching saved to $output_path")
    }
}