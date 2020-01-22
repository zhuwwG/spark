package in.insideRia

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: zhuweiwen@yixin.im
  * @date 2020/1/22 16:21
  */
object Dependencies {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Dependencies").setMaster("local")
    val sc = new SparkContext(conf)
    val links = sc.parallelize(Array(('A',Array('D')),('B',Array('A')),('C',Array('A','B')),('D',Array('A','C'))),2).
      map(x => (x._1,x._2)).cache()
    var ranks = sc.parallelize(Array(('A',1.0),('B',1.0),('C',1.0),('D',1.0)),2)
    for(i <- 1 to 100){
      val contribs = links.join(ranks,2).flatMap{
        case (url, (links, rank)) => links.map(dest => (dest, rank/links.size))
      }
      ranks = contribs.reduceByKey(_ + _, 2).mapValues(0.15 + 0.85 * _)
    }
    ranks.saveAsTextFile("")
  }

}
