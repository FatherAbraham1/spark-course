package graphx

/**
 * Created by gaoyanjie on 2015/4/21.
 */

import java.io.{File, PrintWriter}

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * All information is found from the Spark site :
 * http://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel
 *
 * Structure of Graph Class in Spark.graphx -
 * class Graph[VD, ED] {
 * val vertices: VertexRDD[VD]
 * val edges: EdgeRDD[ED]
 * }
 *
 *
 */

// you can also extend App and then there is no need for a main method
object Graph_PageRank {
  def main(args: Array[String]) {
    /**
     * This is the basic two lines of code needed to setup Spark
     */
    val conf = new SparkConf().setAppName("Graph").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    /**
     * Pre-processing meta-data step
     * Load my user data and parse into tuples of user id and attribute list
     * Split each line by commas and generate a tuple of (uid, list of attributes)
     * （uid，小写名字，大写名字）
     */
    val users : RDD[(Long, Array[String])]= sc.textFile("files/graphx/users.txt")
      .map(line => line.split(","))
      .map(parts => (parts.head.toLong, parts.tail))

    /**
     * Load the edgelist file which is in uid -> uid format
     */
    val followerGraph = GraphLoader.edgeListFile(sc, "files/graphx/followers.txt")

    /**
     * Performs an outer join of the follower graph and the user graph
     * (uid, uid) join (uid, list of attributes) -> ??
     * TODO :: What exactly is happening here? Isn't this just the same as the userlist?
     */
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

    println("\nThe follower graph joined with the user graph\n")
    println(graph.vertices.collect().mkString("\n"))
    /**
     * Subgraph is a graph filter function
     */

    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)
    println("\nThe subgraph\n")
    println(subgraph.vertices.collect().mkString("\n"))
    /**
     * This function calls PageRank.runUntilConvergence which takes the following parameters
     * @param graph the graph on which to compute PageRank
     * @param tol the tolerance allowed at convergence (smaller => more accurate).
     * @param resetProb the random reset probability (alpha)
     */
    val pageRankGraph = subgraph.pageRank(0.001)
    println("\nThe page rank results of the sub-graph\n")
    println(pageRankGraph.vertices.collect().mkString("\n"))
    /**
     * Performs an outer join which is a union operation with an VertexRDD
     */
    val userInfoWithPageRank = subgraph.outerJoinVertices(pageRankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }
    println("\nThe results graph\n")
    println(userInfoWithPageRank.vertices.top(100)(Ordering.by(_._2._1)).mkString("\n"))

  }
}
