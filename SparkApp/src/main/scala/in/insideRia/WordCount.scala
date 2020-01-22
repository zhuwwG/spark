package in.insideRia
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: zhuweiwen@yixin.im
  * @date 2020/1/12 11:38
  */

object WordCount {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://master:8020/user/student/README.txt")
    val words = lines.flatMap(line=>line.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey((x,y)=>x+y)
    wordCounts.foreach(println);
  }
}
