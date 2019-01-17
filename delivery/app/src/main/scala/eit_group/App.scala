package eit_group
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object App {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    if (args.length < 2) {
      println("please provide inputPath as first parameter and outputPath as second parameter")
    } else {
      val inputPath = args(0)
      val outputPath = args(1)
      val conf = new SparkConf().setAppName("My first Spark application")
      val sc = new SparkContext(conf)

      // data is an RDD
//      val data = sc.textFile(s"file:///${inputPath}")
//      println(data.first())
//      println(data.count())

      // let's convert to dataframe
      val spark =
        SparkSession.builder()
          .appName("DataFrame-Basic")
//          .master("local[4]")
          .getOrCreate()
      import spark.implicits._
//      val flightsDF = data.toDF)

      val flightsDF = spark.read.format("csv").option("header", "true").load("file:///"+inputPath)


      println("*** toString() just gives you the schema")

      println(flightsDF.toString())

      println("*** It's better to use printSchema()")

      flightsDF.printSchema()

      println("*** show() gives you neatly formatted data")

      flightsDF.show()

      println("*** use select() to choose one column")

      flightsDF.select("ArrDelay").show()

//      println("*** use select() for multiple columns")
//
//      flightsDF.select("sales", "state").show()
//
//      println("*** use filter() to choose rows")
//
//      flightsDF.filter($"state".equalTo("CA")).show()
      //      val datad = data.todf
//      val numAs = data.filter(line => line.contains("a")).count()
//      val numBs = data.filter(line => line.contains("b")).count()
//      println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
//      data.repartition(1).saveAsTextFile(s"file://${outputPath}")
      sc.stop
    }
  }
}