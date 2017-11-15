/* PageRank.scala */
package assignment1

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
 
object PageRank {

  private var DEFAULT_ITERATION = 60
  private var DEFAULT_BETA = 0.8

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)

    if(args.length < 3) {
      println("Usage: spark-submit --class <className> --master <masterNode> <jar location> <input location> <output location> <modified/simple> <opt: number iterations> <opt: beta>")
      sc.stop()
    }
    //if user input number of iteration for page rank, we override the default.
    if(args.length > 3) {
      PageRank.DEFAULT_ITERATION = args(3).toInt
    }
    //if user input the beta value, we override the default.
    if(args.length > 4) {
      PageRank.DEFAULT_BETA = args(4).toDouble
    }

    val webLinksData = sc.textFile(args(0))

    //emit a pair (fromPage, toPage) entry for each webpage linkage relationship
    val links = webLinksData.filter(line => !line.matches("#.*")).map(s => {
      val pairs = s.split("\t")
      (pairs(0), pairs(1))
    }).distinct().groupByKey().cache()

    //initialise the rank to 1/N
    val numWebpages = links.count();
    var ranks = links.mapValues(v => 1.0/ numWebpages)

    for (i <- 1 to PageRank.DEFAULT_ITERATION) {
      /* links join ranks will give (fromWebId, ([list of toWebIds], rank of fromWebId))
       * By the page rank formula, each toWebpages obtain below weighted rank from fromWebPage
       * = (rank of fromWebPage) / num of outgoing links in fromWebPage
       */
      val ranks_calculated_based_on_fromPages = links.join(ranks).values.flatMap(toNodesAndFromPageRank => {
        val fromPageRank = toNodesAndFromPageRank._2
        val numOutgoingLinksInFromPage = toNodesAndFromPageRank._1.size
        val toPagesList = toNodesAndFromPageRank._1
        toPagesList.map(toPage => {
          val rank_contributedBy_fromPage = fromPageRank / numOutgoingLinksInFromPage;
          (toPage, rank_contributedBy_fromPage)      
        })
      })
      if(args(2).equalsIgnoreCase("modified")) {
        // There are different version of PageRank algorithm calculation.
        // It is referring to formula PR(A) = (1-d) / N + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn)) in https://en.wikipedia.org/wiki/PageRank
        ranks = ranks_calculated_based_on_fromPages.reduceByKey(_ + _).mapValues(rank=> PageRank.DEFAULT_BETA * rank + (1 - PageRank.DEFAULT_BETA)/numWebpages)
      } else {
        ranks = ranks_calculated_based_on_fromPages.reduceByKey(_ + _)
      }
     }

      val result = ranks.map(pageRankResultEntry =>pageRankResultEntry._1 + "\t" + pageRankResultEntry._2)
      result.repartition(1).saveAsTextFile(args(1) + args(2) + " PageRank")

      sc.stop()
    }
}