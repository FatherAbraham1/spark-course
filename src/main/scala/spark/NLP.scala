package spark

/**
 * Created by gaoyanjie on 2015/5/2.
 */
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.Word2Vec

object NLP {
  def main (args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "WordCount", System.getenv("SPARK_HOME"))
//    val input = args.length match {
//      case x: Int if x > 1 => sc.textFile(args(1)).map(line => line.split(" ").toSeq)
//      case _ => sc.parallelize(List("hello world", "i like hello world")).map(line => line.split(" ").toSeq)
//    }
    val input = sc.textFile("files/sql/sampletweets2015.dat").map(line => line.split(" ").toSeq)
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input)
    val synonyms = model.findSynonyms("heart", 40)
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }
  }
}
