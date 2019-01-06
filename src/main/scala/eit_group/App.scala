package eit_group
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

object App {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val inputPath = args(0)
    if (args.length < 2) {
      println("please provide inputPath as first parameter and outputPath as second parameter")
    } else {
      val inputPath = args(0)
      val outputPath = args(1)
      val conf = new SparkConf().setAppName("My first Spark application")
      val sc = new SparkContext(conf)
      val data = sc.textFile(s"file://${inputPath}")
      val numAs = data.filter(line => line.contains("a")).count()
      val numBs = data.filter(line => line.contains("b")).count()
      println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
      data.repartition(1).saveAsTextFile(s"file://${outputPath}")
      sc.stop
    }
  }
}